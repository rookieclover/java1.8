import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;


/**
 * @author: cuiH Date: 13-11-29 自动拆箱功能的实现例子 拆箱过程中包含：享元模式
 */
public class AutoTest {

	private static DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd HH:mm:ss");

	public static void main(String[] args) throws Exception {
		
		//ArrayOfArrayOfString ars ;
   
		
		//List<String[]> lst =  ars
		
		
		
		
		
		
		
		/*
		
		
		
		SegmentManager sm = SegmentManager.getInstance();
		
		
		String s = sm.segment("<table><table>asdfasdf中文件");
		

System.out.println(s);*/
		
	}
}

class DateFormatThread extends Thread
{
	private DateFormat dateFormat = null;
	public DateFormatThread(DateFormat dateFormat)
	{
		this.dateFormat = dateFormat;
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		
		for(int i =0 ;i < 10 ; i ++)
		{
			try {
				dateFormat.parse("20101010 10:22:00");
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}
	
}

/**
 * 仓库
 */
class Godown {
	public static final int max_size = 100; // 最大库存量
	public int curnum; // 当前库存量

	Godown() {
	}

	Godown(int curnum) {
		this.curnum = curnum;
	}

	/**
	 * 生产指定数量的产品
	 * 
	 * @param neednum
	 */
	public synchronized void produce(int neednum) {
		// 测试是否需要生产
		while (neednum + curnum > max_size) {
//			System.out.println("要生产的产品数量" + neednum + "超过剩余库存量"
//					+ (max_size - curnum) + "，暂时不能执行生产任务!");
			try {
				// 当前的生产线程等待
				wait();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		// 满足生产条件，则进行生产，这里简单的更改当前库存量
		curnum += neednum;
		System.out.println("已经生产了" + neednum + "个产品，现仓储量为" + curnum);
		// 唤醒在此对象监视器上等待的所有线程
		notifyAll();
	}

	/**
	 * 消费指定数量的产品
	 * 
	 * @param neednum
	 */
	public synchronized void consume(int neednum) {
		// 测试是否可消费
		while (curnum < neednum) {
			try {
				// 当前的生产线程等待
				wait();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		// 满足消费条件，则进行消费，这里简单的更改当前库存量
		curnum -= neednum;
		System.out.println("已经消费了" + neednum + "个产品，现仓储量为" + curnum);
		// 唤醒在此对象监视器上等待的所有线程
		notifyAll();
	}
}

/**
 * 生产者
 */
class Producer extends Thread {
	private int neednum; // 生产产品的数量
	private Godown godown; // 仓库

	Producer(int neednum, Godown godown) {
		this.neednum = neednum;
		this.godown = godown;
	}

	public void run() {
		// 生产指定数量的产品
		godown.produce(neednum);
	}
}

/**
 * 消费者
 */
class Consumer extends Thread {
	private int neednum; // 生产产品的数量
	private Godown godown; // 仓库

	Consumer(int neednum, Godown godown) {
		this.neednum = neednum;
		this.godown = godown;
	}

	public void run() {
		// 消费指定数量的产品
		godown.consume(neednum);
	}
}

class KepperP implements Runnable
{
	Godown g;
	Thread t ;
	public KepperP(Godown g)
	{
		this.g=g;
		t = new Thread(this);
		t.start();
	
	}
	@Override
	public void run() {
		// TODO Auto-generated method stub
		while(true)
		{
			Producer p1 = new Producer(10, g);
			p1.start();
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}

class KepperC implements Runnable
{
	Godown g;
	Thread t ;
	public KepperC(Godown g)
	{
		this.g=g;
		t = new Thread(this);
		t.start();
	
	}
	@Override
	public void run() {
		// TODO Auto-generated method stub
		while(true)
		{
			Consumer p1 = new Consumer(1, g);
			p1.start();
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
