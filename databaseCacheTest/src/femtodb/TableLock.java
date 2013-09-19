package femtodb;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

/** A Lock implementation with the added feature that the thread calling its backupLock method (to acquire the lock) is given priority over threads calling other methods. */
public class TableLock implements java.util.concurrent.locks.Lock{

	private Thread 			currentLockHolder 	= null;
	private Thread 			backupLock 			= null;
	private List<Thread> 	waitingThreads 		= new ArrayList<Thread>();
	private int				holdCount 			= 0;
	
	/** Blocks until the lock has been obtained. If the calling thread gets interrupted the method still continues blocking until the lock has been obtained. */ 
	@Override
	synchronized
	public void lock() {
		
		if(currentLockHolder == null)
		{
			currentLockHolder = Thread.currentThread();
			holdCount++;
			return;
		}
		Thread currentThread = Thread.currentThread();
		if(currentLockHolder == currentThread)
		{
			holdCount++;
			return;		
		}
		waitingThreads.add(currentThread);
		
		// keep blocking until notify gets called on the thread regardless of InteruptExceptions
		boolean keepLocked = true;
		while(keepLocked)
		{
			try {
				currentThread.wait();
				keepLocked = false;
			} catch (InterruptedException e) {}
		}
	}
	
	/** Blocks until the lock is acquired for backing up or shutting down. This has the highest priority lock request. */
	synchronized
	public void backupLock() {
		
		if(currentLockHolder == null)
		{
			currentLockHolder = Thread.currentThread();
			holdCount++;
			return;
		}
		Thread currentThread = Thread.currentThread();
		if(currentLockHolder == currentThread)
		{
			holdCount++;
			return;
		}
		backupLock = currentThread;
		try {
			currentThread.wait();
		} catch (InterruptedException e) {}	
	}

	/** Blocks until the lock has been obtained. If the calling thread gets interrupted the method will throw an InterruptedException. */ 
	@Override
	synchronized
	public void lockInterruptibly() throws InterruptedException {
		if(currentLockHolder == null)
		{
			currentLockHolder = Thread.currentThread();
			holdCount++;
			return;
		}
		Thread currentThread = Thread.currentThread();
		if(currentLockHolder == currentThread)
		{
			holdCount++;
			return;
		}
		waitingThreads.add(currentThread);
		currentThread.wait();		
	}

	/** Attempts to acquire the lock returning immediately. If there are no threads currently holding the lock then the caller is given the lock and the method will return true. */
	@Override
	synchronized
	public boolean tryLock() {
		if(currentLockHolder == null)
		{
			currentLockHolder = Thread.currentThread();
			holdCount++;
			return true;
		}
		return false;
	}

	/** Attempts to acquire the lock with a timeout. Returns true if the lock was acquired */
	@Override
	synchronized
	public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
		// return true if not locked
		if(currentLockHolder == null)
		{
			currentLockHolder = Thread.currentThread();
			holdCount++;
			return true;
		}
		final Thread currentThread = Thread.currentThread();
		if(currentLockHolder == currentThread)
		{
			holdCount++;
			return true;
		}
		
		// calculate the timeout time in milliseconds
		if(unit == TimeUnit.NANOSECONDS) time = time / 1000000L;
		if(unit == TimeUnit.MICROSECONDS)time = time / 1000L;
		if(unit == TimeUnit.SECONDS)time = time * 1000L;
		if(unit == TimeUnit.MINUTES)time = time * 1000L * 60L;
		if(unit == TimeUnit.HOURS)time = time * 1000L * 60L * 60L;
		if(unit == TimeUnit.DAYS)time = time * 1000L * 60L * 60L * 24L;
		
		final boolean[] expired = { false };
		final long time2 = time;
		
		// create a timeout thread
		Thread timeoutThread = new Thread()
		{
			@Override
			public void run()
			{
				try {
					Thread.sleep(time2);
				} catch (InterruptedException e) {}
				expired[0] = true;
				currentThread.notify();
			}
		};
		
		// use wait on current thread to block until lock is acquired or
		waitingThreads.add(currentThread);
		timeoutThread.start();
		currentThread.wait();		
		if(expired[0])
		{
			// If we acquired the lock as expired went true
			// we need to return true, to ensure the caller calls unlock later.
			return(currentLockHolder == currentThread);
		}
		else
		{
			return true;
		}
	}
	
	/** If the calling thread owns the lock, its hold count is decreased by one. 
	 * If the count reaches zero other threads acquire the lock in the order
	 * they arrived. However if there is a caller to backupLock it always given priority.
	 */
	@Override
	synchronized
	public void unlock() {
		
		// return if we do not hold the lock
		Thread currentLockHolderL = currentLockHolder;
		final Thread currentThread = Thread.currentThread();
		if(currentLockHolderL != currentThread)return;
		
		// decrement hold count
		holdCount--;
		if(holdCount > 0)return;
		
		

		if(backupLock == null)
		{
			if(waitingThreads.isEmpty())
			{
				currentLockHolder = null;
			}
			else
			{
				currentLockHolder = waitingThreads.get(0);
				waitingThreads.remove(0);
				holdCount++;
				currentLockHolder.notify();		
			}
		}
		else
		{
			currentLockHolder = backupLock;
			backupLock = null;
			holdCount++;
			currentLockHolder.notify();	
		}	
	}

	@Override
	public Condition newCondition() {
		// NOT IMPLEMENTED
		return null;
	}
	
	/** Returns the number of holds on this lock by the thread that currently owns it */
	synchronized
	public final int getHoldCount() {
		return holdCount;
	}
	
	/** Returns the number of threads waiting on this lock (including the backup thread) */
	synchronized
	public final int getQueueLength()
	{
		int retval = waitingThreads.size();
		if(backupLock != null)retval++;
		return retval;
	}
	
	/** Returns true if the thread given as an argument is waiting for this lock */
	synchronized
	public final boolean hasQueuedThread(Thread t)
	{
		if((t != null)&&(backupLock == t))return true;
		return waitingThreads.contains(t);
	}
	
	/** Returns true if threads are waiting on this lock */
	synchronized
	public final boolean hasQueuedThreads()	
	{
		if(backupLock != null)return true;
		return (!waitingThreads.isEmpty());
	}
	
	/** Returns true if the lock is currently held by the calling thread */
	synchronized
	public boolean isHeldByCurrentThread()
	{
		if((currentLockHolder != null)&&(currentLockHolder == Thread.currentThread()))return true;
		return false;
	}
	
	/** Returns true if a thread currently holds the lock */
	synchronized
	public boolean isLocked()
	{
		if(currentLockHolder == null)return true;
		return false;
	}
	
	
	
	
	
	

	
}
