package femtodb;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

/** A Lock implementation with the added feature that the thread calling its backupLock method (to acquire the lock) is given priority over threads calling other methods. */
public class TableLock implements java.util.concurrent.locks.Lock{

	private Thread currentLockHolder 	= null;
	private Thread backupLock 			= null;
	private List<Thread> waitingThreads = new ArrayList<Thread>();
	
	@Override
	synchronized
	public void lock() {
		
		if(currentLockHolder == null)
		{
			currentLockHolder = Thread.currentThread();
			return;
		}
		Thread currentThread = Thread.currentThread();
		if(currentLockHolder == currentThread)return;		
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
	
	/** Blocks until the lock is acquired for backing up or shutting down. This has highest priority lock request. */
	synchronized
	public void backupLock() {
		
		if(currentLockHolder == null)
		{
			currentLockHolder = Thread.currentThread();
			return;
		}
		Thread currentThread = Thread.currentThread();
		if(currentLockHolder == currentThread)return;
		backupLock = currentThread;
		try {
			currentThread.wait();
		} catch (InterruptedException e) {}	
	}

	@Override
	synchronized
	public void lockInterruptibly() throws InterruptedException {
		if(currentLockHolder == null)
		{
			currentLockHolder = Thread.currentThread();
			return;
		}
		Thread currentThread = Thread.currentThread();
		if(currentLockHolder == currentThread)return;
		waitingThreads.add(currentThread);
		currentThread.wait();		
	}

	@Override
	synchronized
	public boolean tryLock() {
		if(currentLockHolder == null)
		{
			currentLockHolder = Thread.currentThread();
			return true;
		}
		return false;
	}

	@Override
	synchronized
	public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
		// return true if not locked
		if(currentLockHolder == null)
		{
			currentLockHolder = Thread.currentThread();
			return true;
		}
		final Thread currentThread = Thread.currentThread();
		if(currentLockHolder == currentThread)return true;
		
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

	@Override
	synchronized
	public void unlock() {
		final Thread currentThread = Thread.currentThread();
		Thread currentLockHolderL = currentLockHolder;
		if(currentLockHolderL != currentThread)return;	
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
				currentLockHolder.notify();		
			}
		}
		else
		{
			currentLockHolder = backupLock;
			backupLock = null;
			currentLockHolder.notify();	
		}	
	}

	@Override
	public Condition newCondition() {
		// NOT IMPLEMENTED
		return null;
	}

	
}
