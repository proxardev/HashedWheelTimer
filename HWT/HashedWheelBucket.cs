using Microsoft.Extensions.ObjectPool;
using System;
using System.Collections.Generic;

namespace HWT
{


    internal class TimerNodePolicy : PooledObjectPolicy<LinkedListNode<long>>
    {
        public override LinkedListNode<long> Create() => new LinkedListNode<long>(0);

        public override bool Return(LinkedListNode<long> obj)
        {
            obj.Value = 0;
            return true;
        }
    }

    internal sealed class HashedWheelBucket
    {
        private HashedWheelTimer hashedWheelTimer;

        // Used for the linked-list datastructure
        private LinkedList<long> timeoutIdLink = new LinkedList<long>();

        public HashedWheelBucket(HashedWheelTimer timer)
        {
            this.hashedWheelTimer = timer;
        }

        public static LinkedListNode<long> RentNode()
        {
            return pools.Get();
        }

        private static readonly ObjectPool<LinkedListNode<long>> pools = new DefaultObjectPoolProvider() { MaximumRetained = 10000 }.Create(new TimerNodePolicy());


        internal void ReturnToPools(LinkedListNode<long> linkedListNode)
        {
            pools.Return(linkedListNode);
        }

        public void AddTimeout(HashedWheelTimeout timeout)
        {
            timeout._bucket = this;
            var node = RentNode();
            node.Value = timeout.Id;
            timeoutIdLink.AddLast(node);
        }

        /// <summary>
        /// Expire all HashedWheelTimeout for the given deadline.
        /// </summary>
        /// <param name="deadline"></param>
        internal void ExpireTimeouts(long deadline)
        {
            var node = timeoutIdLink.First;

            // process all timeouts
            while (node != null)
            {
                var id = node.Value;
                var next = node.Next;
                try
                {
                    if (!this.hashedWheelTimer._timeoutMaps.TryGetValue(id, out var timeout))
                    {
                        this.Remove(node);
                        continue;
                    }
                    if (timeout._remainingRounds <= 0)
                    {
                        this.Remove(node);
                        this.hashedWheelTimer._timeoutMaps.Remove(id);
                        if (timeout._deadline <= deadline)
                        {
                            timeout.Expire();
                        }
                        else
                        {
                            // The timeout was placed into a wrong slot. This should never happen.
                            throw new InvalidOperationException($"timeout.deadline ({timeout._deadline}) > deadline ({deadline})");
                        }
                    }
                    else if (timeout.Cancelled)
                    {
                        this.Remove(node);
                    }
                    else
                    {
                        timeout._remainingRounds--;
                    }
                }
                finally
                {
                    node = next;
                }
            }
        }

        internal void Remove(LinkedListNode<long> node)
        {
            this.timeoutIdLink.Remove(node);
            ReturnToPools(node);
            this.hashedWheelTimer.DescreasePendingTimeouts();
        }

        internal void ClearTimeouts(ISet<Timeout> set)
        {
            var node = this.timeoutIdLink.First;
            for (; ; )
            {
                if (node == null)
                {
                    break;
                }
                var next = node.Next;
                try
                {
                    if (!this.hashedWheelTimer._timeoutMaps.TryGetValue(node.Value, out var timeout))
                    {
                        continue;
                    }
                    if (timeout == null)
                    {
                        return;
                    }
                    if (timeout.Expired)
                    {
                        continue;
                    }
                    if (timeout.Cancelled)
                    {
                        continue;
                    }
                    set.Add(timeout);
                }
                finally
                {
                    node = next;
                }
            }
        }
    }
}
