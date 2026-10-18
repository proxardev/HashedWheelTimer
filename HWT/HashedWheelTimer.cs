using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;


namespace HWT
{

    /// <summary>
    /// A Timer optimized for approximated I/O timeout scheduling.
    /// 
    /// ## Tick Duration ##
    /// As described with 'approximated', this timer does not execute the scheduled TimerTask on time. hashedWheelTimer, 
    /// on every tick, will check if there are any TimerTasks behind the schedule and execute them.
    /// You can increase or decrease the accuracy of the execution timing by specifying smaller or larger tick duration 
    /// in the constructor.In most network applications, I/O timeout does not need to be accurate. 
    /// Therefore, the default tick duration is 100 milliseconds and you will not need to try different configurations in most cases.
    /// 
    /// ## Ticks per Wheel (Wheel Size) ##
    /// 
    /// hashedWheelTimer maintains a data structure called 'wheel'. 
    /// To put simply, a wheel is a hash table of TimerTasks whose hash function is 'dead line of the task'. 
    /// The default number of ticks per wheel (i.e. the size of the wheel) is 512. 
    /// You could specify a larger value if you are going to schedule a lot of timeouts.
    /// 
    /// ## Do not create many instances. ##
    /// 
    /// hashedWheelTimer creates a new thread whenever it is instantiated and started. 
    /// Therefore, you should make sure to create only one instance and share it across your application. 
    /// One of the common mistakes, that makes your application unresponsive, is to create a new instance for every connection.
    /// 
    /// ## Implementation Details ##
    /// hashedWheelTimer is based on George Varghese and Tony Lauck's paper, 
    /// 'Hashed and Hierarchical Timing Wheels: data structures to efficiently implement a timer facility'. 
    /// More comprehensive slides are located here http://www.cse.wustl.edu/~cdgill/courses/cs6874/TimingWheels.ppt.
    /// </summary>
    public partial class HashedWheelTimer : Timer
    {
        public const int WORKER_STATE_INIT = 0;
        public const int WORKER_STATE_STARTED = 1;
        public const int WORKER_STATE_SHUTDOWN = 2;

        private volatile int _workerState; // 0 - init, 1 - started, 2 - shut down

        private readonly long _tickDuration;
        private readonly HashedWheelBucket[] _wheel;
        private readonly int _mask;
        private readonly ManualResetEvent _startTimeInitialized = new ManualResetEvent(false);
        private readonly ConcurrentQueue<HashedWheelTimeout> _timeouts = new ConcurrentQueue<HashedWheelTimeout>();
        private readonly Queue<HashedWheelTimeout> _timeouts2 = new Queue<HashedWheelTimeout>();
        internal readonly Dictionary<long, HashedWheelTimeout> _timeoutMaps = new Dictionary<long, HashedWheelTimeout>();
        internal readonly ConcurrentQueue<HashedWheelTimeout> _cancelledTimeouts = new ConcurrentQueue<HashedWheelTimeout>();
        internal readonly ConcurrentQueue<(long, Action<long, bool>)> _cancelledIds = new ConcurrentQueue<(long, Action<long, bool>)>();
        internal readonly ConcurrentQueue<(long, TimerTask, Action<long, TimerTask, bool>)> _cancelledIds2 = new ConcurrentQueue<(long, TimerTask, Action<long, TimerTask, bool>)>();
        private readonly HashSet<long> CheckCancelledIds = new HashSet<long>();
        private readonly long _maxPendingTimeouts;
        private readonly Thread _workerThread;

        /// <summary>
        /// There are 10,000 ticks in a millisecond
        /// </summary>
        private readonly long _base = DateTime.UtcNow.Ticks / 10000;


        private /*volatile*/ long _startTime;
        private long _pendingTimeouts = 0;

        private long GetCurrentMs() { return DateTime.UtcNow.Ticks / 10000 - _base; }

        internal long DescreasePendingTimeouts()
        {
            return Interlocked.Decrement(ref _pendingTimeouts);
        }

        /// <summary>
        /// Creates a new timer with default tick duration 100 ms, and default number of ticks per wheel 512.
        /// </summary>
        public HashedWheelTimer() : this(TimeSpan.FromMilliseconds(100), 512, 0)
        {
        }

        /// <summary>
        /// Creates a new timer.
        /// </summary>
        /// <param name="tickDuration">the duration between tick</param>
        /// <param name="ticksPerWheel">the size of the wheel</param>
        /// <param name="maxPendingTimeouts">The maximum number of pending timeouts after which call to NewTimeout will result in InvalidOperationException being thrown. No maximum pending timeouts limit is assumed if this value is 0 or negative.
        /// </param>
        public HashedWheelTimer(TimeSpan tickDuration, int ticksPerWheel, long maxPendingTimeouts)
        {

            if (tickDuration == null)
            {
                throw new ArgumentNullException(nameof(tickDuration), "must be greater than 0");
            }
            if (tickDuration.TotalMilliseconds <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(tickDuration), "must be greater than 0 ms");
            }
            if (ticksPerWheel <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(ticksPerWheel), "must be greater than 0: ");
            }

            // Normalize ticksPerWheel to power of two and initialize the wheel.
            _wheel = CreateWheel(ticksPerWheel);
            _mask = _wheel.Length - 1;

            // Convert tickDuration to ms.
            this._tickDuration = (long)tickDuration.TotalMilliseconds;

            // Prevent overflow.
            if (this._tickDuration >= long.MaxValue / _wheel.Length)
            {
                throw new ArgumentOutOfRangeException(nameof(tickDuration)
                    , $"{tickDuration} (expected: 0 < tickDuration in ms < {long.MaxValue / _wheel.Length}");
            }
            _workerThread = new Thread(this.Run);

            this._maxPendingTimeouts = maxPendingTimeouts;
        }


        private HashedWheelBucket[] CreateWheel(int ticksPerWheel)
        {
            if (ticksPerWheel <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(ticksPerWheel), "must be greater than 0");
            }
            if (ticksPerWheel > 1073741824)
            {
                throw new ArgumentOutOfRangeException(nameof(ticksPerWheel), "may not be greater than 2^30");
            }

            ticksPerWheel = NormalizeTicksPerWheel(ticksPerWheel);
            HashedWheelBucket[] wheel = new HashedWheelBucket[ticksPerWheel];
            for (int i = 0; i < wheel.Length; i++)
            {
                wheel[i] = new HashedWheelBucket(this);
            }
            return wheel;
        }

        private static int NormalizeTicksPerWheel(int ticksPerWheel)
        {
            int normalizedTicksPerWheel = 1;
            while (normalizedTicksPerWheel < ticksPerWheel)
            {
                normalizedTicksPerWheel <<= 1;
            }
            return normalizedTicksPerWheel;
        }

        /// <summary>
        ///  Starts the background thread explicitly.  The background thread will start automatically on demand 
        ///  even if you did not call this method.
        /// </summary>
        private void Start()
        {
            switch (_workerState)
            {
                case WORKER_STATE_INIT:
                    int originalWorkerState = Interlocked.CompareExchange(ref _workerState, WORKER_STATE_STARTED, WORKER_STATE_INIT);
                    if (originalWorkerState == WORKER_STATE_INIT)
                    {
                        _workerThread.Start();
                    }
                    break;
                case WORKER_STATE_STARTED:
                    break;
                case WORKER_STATE_SHUTDOWN:
                    return;
                default:
                    throw new InvalidOperationException("HashedWheelTimer.workerState is invalid");
            }

            // Wait until the startTime is initialized by the worker.
            while (_startTime == 0)
            {
                try
                {
                    _startTimeInitialized.WaitOne(5000);
                }
                catch
                {
                    // Ignore - it will be ready very soon.
                }
            }
        }

        /// <summary>
        /// Schedules the specified TimerTask for one-time execution after the specified delay.
        /// </summary>
        /// <param name="task"></param>
        /// <param name="span"></param>
        /// <returns>a handle which is associated with the specified task</returns>
        public long NewTimeout(TimerTask task, TimeSpan span)
        {
            if (task == null)
            {
                throw new ArgumentNullException(nameof(task));
            }

            if (_workerState == WORKER_STATE_SHUTDOWN)
                return 0;

            long pendingTimeoutsCount = Interlocked.Increment(ref _pendingTimeouts);

            if (_maxPendingTimeouts > 0 && pendingTimeoutsCount > _maxPendingTimeouts)
            {
                Interlocked.Decrement(ref _pendingTimeouts);
                throw new InvalidOperationException($"Number of pending timeouts ({pendingTimeoutsCount}) is greater than or equal to maximum allowed pending  timeouts ({_maxPendingTimeouts})");
            }

            Start();

            // Add the timeout to the timeout queue which will be processed on the next tick.
            // During processing all the queued HashedWheelTimeouts will be added to the correct HashedWheelBucket.
            long deadline = GetCurrentMs() + (long)span.TotalMilliseconds - _startTime;

            // Guard against overflow.
            if (span.TotalMilliseconds > 0 && deadline < 0)
            {
                deadline = long.MaxValue;
            }
            HashedWheelTimeout timeout = HashedWheelTimeout.Rent().Init(this, task, deadline);
            _timeouts.Enqueue(timeout);
            return timeout.Id;
        }

        private void ProcessAddTimeout()
        {
            for (; ; )
            {
                if (!_timeouts.TryDequeue(out var data))
                {
                    break;
                }
                this._timeouts2.Enqueue(data);
                _timeoutMaps.Add(data.Id, data);
            }
        }

        /// <summary>
        /// Releases all resources acquired by this Timer and cancels all tasks which were scheduled but not executed yet.
        /// </summary>
        /// <returns></returns>
        public ISet<long> Stop()
        {
            int originalWorkerState = Interlocked.CompareExchange(ref _workerState, WORKER_STATE_SHUTDOWN, WORKER_STATE_STARTED);

            if (originalWorkerState != WORKER_STATE_STARTED)
            {
                return new HashSet<long>();
            }

            try
            {
                while (_workerThread.IsAlive)
                {
                    _workerThread.Join(1000);
                }
            }
            catch
            {

            }
            var results = new HashSet<long>(_unprocessedTimeouts.Select(x => x.Id));
            return results;
        }



        private readonly ISet<Timeout> _unprocessedTimeouts = new HashSet<Timeout>();
        private long _tick;

        private void Run()
        {
            // Initialize the startTime.
            _startTime = GetCurrentMs();
            if (_startTime == 0)
            {
                // We use 0 as an indicator for the uninitialized value here, so make sure it's not 0 when initialized.
                _startTime = 1;
            }

            // Notify the other threads waiting for the initialization at start().
            _startTimeInitialized.Set();

            do
            {
                long deadline = WaitForNextTick();
                if (deadline > 0)
                {
                    int idx = (int)(_tick & _mask);
                    ProcessAddTimeout();
                    ProcessCancelledIds();
                    ProcessCancelledTasks();
                    HashedWheelBucket bucket = _wheel[idx];
                    TransferTimeoutsToBuckets();
                    bucket.ExpireTimeouts(deadline);
                    _tick++;
                }
            } while (_workerState == WORKER_STATE_STARTED);

            // Fill the unprocessedTimeouts so we can return them from stop() method.
            foreach (HashedWheelBucket bucket in _wheel)
            {
                bucket.ClearTimeouts(_unprocessedTimeouts);
            }
            for (; ; )
            {
                if (_timeouts2.Count == 0)
                {
                    break;
                }
                var timeout = _timeouts2.Dequeue();
                var timeoutId = timeout.Id;
                if (!_timeoutMaps.TryGetValue(timeoutId, out timeout))
                {
                    continue;
                }
                _timeoutMaps.Remove(timeoutId);
                if (!timeout.Cancelled)
                {
                    _unprocessedTimeouts.Add(timeout);
                }
            }
            ProcessCancelledTasks();
        }

        private void TransferTimeoutsToBuckets()
        {
            // transfer only max. 100000 timeouts per tick to prevent a thread to stale the workerThread when it just
            // adds new timeouts in a loop.
            var over = false;
            for (int i = 0; i < 100000; i++)
            {
                if (_timeouts2.Count == 0)
                {
                    // all processed
                    over = true;
                    break;
                }
                var timeout = _timeouts2.Dequeue();

                var timeoutId = timeout.Id;
                if (this.CheckCancelledIds.Contains(timeoutId))
                {
                    continue;
                }
                if (!_timeoutMaps.TryGetValue(timeoutId, out timeout))
                {
                    continue;
                }
                if (timeout.Cancelled)
                {
                    // Was cancelled in the meantime.
                    continue;
                }

                long calculated = timeout._deadline / _tickDuration;
                timeout._remainingRounds = (calculated - _tick) / _wheel.Length;

                long ticks = Math.Max(calculated, _tick); // Ensure we don't schedule for past.
                int stopIndex = (int)(ticks & _mask);

                HashedWheelBucket bucket = _wheel[stopIndex];
                bucket.AddTimeout(timeout);
            }
            if (over)
            {
                this.CheckCancelledIds.Clear();
            }
        }


        private void ProcessCancelledTasks()
        {
            for (; ; )
            {
                if (!_cancelledTimeouts.TryDequeue(out var timeout))
                {
                    // all processed
                    break;
                }
                timeout.ReturnToPools();
            }
        }


        private long WaitForNextTick()
        {
            long deadline = _tickDuration * (_tick + 1);

            for (; ; )
            {
                long currentTime = GetCurrentMs() - _startTime;
                int sleepTimeMs = (int)Math.Truncate(deadline - currentTime + 1M);

                if (sleepTimeMs <= 0)
                {
                    if (currentTime == long.MaxValue)
                    {
                        return -long.MaxValue;
                    }
                    else
                    {
                        return currentTime;
                    }
                }

                Thread.Sleep(sleepTimeMs);
            }
        }

        public TimedAwaiter Delay(long milliseconds)
        {
            TimedAwaiter awaiter = new TimedAwaiter();
            this.NewTimeout(awaiter, TimeSpan.FromMilliseconds(milliseconds));
            return awaiter;
        }

        public void TryCancel(long id, Action<long, bool> cancelResultInvoke)
        {
            this._cancelledIds.Enqueue((id, cancelResultInvoke));
        }

        private void ProcessCancelledIds()
        {
            for (; ; )
            {
                if (!_cancelledIds.TryDequeue(out var data))
                {
                    break;
                }
                var (id, cancelResultInvoke) = data;
                this.CancelTimer(id, cancelResultInvoke);
                this.CheckCancelledIds.Add(id);
            }
            for (; ; )
            {
                if (!_cancelledIds2.TryDequeue(out var data))
                {
                    break;
                }
                var (id, timerTask, cancelResultInvoke) = data;
                this.CancelTimer(id, timerTask, cancelResultInvoke);
                this.CheckCancelledIds.Add(id);
            }
        }

        private void CancelTimer(long id, Action<long, bool> cancelResultInvoke)
        {
            var succ = false;
            if (this._timeoutMaps.TryGetValue(id, out var timeout))
            {
                this._timeoutMaps.Remove(id);
                succ = timeout.Cancel();
            }
            cancelResultInvoke?.Invoke(id, succ);
        }


        public void TryCancel(long id, TimerTask timerTask, Action<long, TimerTask, bool> cancelResultInvoke)
        {
            this._cancelledIds2.Enqueue((id, timerTask, cancelResultInvoke));
        }

        private void CancelTimer(long id, TimerTask timerTask, Action<long, TimerTask, bool> cancelResultInvoke)
        {
            var succ = false;
            if (this._timeoutMaps.TryGetValue(id, out var timeout))
            {
                this._timeoutMaps.Remove(id);
                succ = timeout.Cancel();
            }
            cancelResultInvoke?.Invoke(id, timerTask, succ);
        }
    }

}
