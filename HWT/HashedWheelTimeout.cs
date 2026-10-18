using Microsoft.Extensions.ObjectPool;
using System.Threading;

namespace HWT
{
    internal sealed class HashedWheelTimeout : Timeout, IResettable
    {
        internal const int ST_INIT = 0;
        internal const int ST_CANCELLED = 1;
        internal const int ST_EXPIRED = 2;

        private volatile int _state = ST_INIT;

        internal HashedWheelTimer _timer { get; private set; }
        private TimerTask _task { get; set; }
        internal long _deadline { get; private set; }

        // remainingRounds will be calculated and set by Worker.transferTimeoutsToBuckets() before the
        // HashedWheelTimeout will be added to the correct HashedWheelBucket.
        internal long _remainingRounds;

        // The bucket to which the timeout was added
        internal HashedWheelBucket _bucket;



        public bool TryReset()
        {
            this._bucket = null;
            this._remainingRounds = 0;
            this._timer = null;
            Interlocked.Exchange(ref _state, ST_INIT);
            this._task = null;
            this._deadline = 0;
            this.Id = 0;
            return true;
        }

        internal HashedWheelTimeout Init(HashedWheelTimer timer, TimerTask task, long deadline)
        {
            this.Id = NewId();
            this._timer = timer;
            this._task = task;
            this._deadline = deadline;
            return this;
        }


        public Timer Timer { get { return _timer; } }

        public TimerTask TimerTask { get { return _task; } }

        public bool Expired { get { return _state == ST_EXPIRED; } }

        public bool Cancelled { get { return _state == ST_CANCELLED; } }

        public long Id { get; private set; }

        private static long idCounter = 1;

        private static long NewId() => Interlocked.Increment(ref idCounter);

        bool CompareAndSetState(int expected, int state)
        {
            int originalState = Interlocked.CompareExchange(ref _state, state, expected);
            return originalState == expected;
        }

        public bool Cancel()
        {
            // only update the state it will be removed from HashedWheelBucket on next tick.
            if (!CompareAndSetState(ST_INIT, ST_CANCELLED))
            {
                return false;
            }
            // If a task should be canceled we put this to another queue which will be processed on each tick.
            // So this means that we will have a GC latency of max. 1 tick duration which is good enough. This way
            // we can make again use of our MpscLinkedQueue and so minimize the locking / overhead as much as possible.
            this._timer._cancelledTimeouts.Enqueue(this);
            return true;
        }

        public void Expire()
        {
            if (!CompareAndSetState(ST_INIT, ST_EXPIRED))
            {
                return;
            }

            try
            {
                _task.Run(this.Id);
            }
            finally
            {
                this.ReturnToPools();
            }
        }

        private static readonly ObjectPool<HashedWheelTimeout> pools = new DefaultObjectPoolProvider() { MaximumRetained = 10000 }.Create<HashedWheelTimeout>();
        internal static HashedWheelTimeout Rent()
        {
            return pools.Get();
        }

        internal void ReturnToPools()
        {
            pools.Return(this);
        }
    }
}
