package org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;

import org.apache.commons.math3.analysis.function.Min;

public class GPUMonitor implements Runnable {
	boolean bIsGpuAvailable = true;
	boolean bRunning = true;
	int desired = 0;
	STATE state = STATE.EMPTY;
	float upper_threshold = 96.0f;
	float lower_threshold = 10.0f;
	int memory;
	int processCount;
	float util_avg;
	int max_num_of_gpu_yarnchild = 8;
	boolean bNumOverMaxGpu = false;
	String debug_listener_address; 
	boolean bUse_debug_listener;
	int max_gpu;
	int dynamic_policy = 0;
	int num_min_gpu_maptask = 0;
	boolean print_state=false;
	boolean cpumaptaskisover =false;
	boolean max_start_mode = true;
	public float X = 99999;

	public GPUMonitor(String debug_listener_address, boolean bUse_debug_listener) {
		// TODO Auto-generated constructor stub
		this.debug_listener_address = debug_listener_address;
		this.bUse_debug_listener = bUse_debug_listener;
	}

	public GPUMonitor(String debug_listener_address2,
			boolean bUse_debug_listener2, float upper_threshold2) {
		// TODO Auto-generated constructor stub
		this( debug_listener_address2,bUse_debug_listener2);
		this.upper_threshold = upper_threshold2;
	}
	
	public GPUMonitor(String debug_listener_address2,
			boolean bUse_debug_listener2, float upper_threshold2,int dynamic_policy) {
		// TODO Auto-generated constructor stub
		this( debug_listener_address2,bUse_debug_listener2 , dynamic_policy);
		this.dynamic_policy = dynamic_policy;
	}
	
	public void CpuMapTaskIsOver(){
		cpumaptaskisover = true;
	}
	public void SetNumMinGpuMapTask(int num_min_gpu_maptask){
		this.num_min_gpu_maptask = num_min_gpu_maptask;
	}
	public void SetMaxStartMode(boolean max_start_mode){
		this.max_start_mode = max_start_mode;
	}
	

	void sendMsg(String mes) {
		String full_mes = "";
		full_mes += "<<<<<<<<<\n";
		full_mes += mes;
		full_mes += "\n>>>>>\n";
		Socket s;
		try {
			s = new Socket(debug_listener_address, 51231);
			PrintWriter out = new PrintWriter(s.getOutputStream(), true);
			out.println(full_mes);
			s.close();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			System.out.println("UnknownHostException!");
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("IOException!");
			e.printStackTrace();
		}
	}
	public boolean FSM_conservative(int cur_gpu,int max_gpu,boolean isNewMapTask) {
		processCount = cur_gpu;
		this.max_gpu = max_gpu;

		switch (state) {
		case EMPTY:
			bIsGpuAvailable = true;
			desired = num_min_gpu_maptask;
			
			if( cur_gpu >= 1 ){
				//desired = max_gpu;
				//bNumOverMaxGpu = true;
				state = STATE.WAIT;
				bIsGpuAvailable = true;
			}
			break;
		case INCRE:
			bIsGpuAvailable = true;
			if( cur_gpu >= desired ){
				state = STATE.WAIT;
				bIsGpuAvailable = false;
			}else if( cur_gpu==0){
				state = STATE.EMPTY;
			}
			break;	
		case WAIT:
			bIsGpuAvailable = false;
			if( cur_gpu < desired ){
				state = STATE.INCRE;
			}
			if( cpumaptaskisover ){
				cpumaptaskisover = false;
				desired++;
				state = STATE.INCRE;
			}
			if( util_avg > upper_threshold ){
				state = STATE.FULL;
				bIsGpuAvailable = false;
			}
			break;				
			
		case TRANSITION1:
			if( util_avg > upper_threshold ){
			//if( cur_gpu >= desired ){
				state = STATE.FULL;
				bIsGpuAvailable = false;
			}else if ( cur_gpu <= desired - 2 ){
				state = STATE.UNDER;
				desired = max_gpu;
				bIsGpuAvailable = true;
			}
			break;
		case FULL:
			if( util_avg <= upper_threshold ){
				state = STATE.TRANSITION2;
				bIsGpuAvailable = true;
				desired = cur_gpu+2>max_gpu ? max_gpu : cur_gpu+2;
				X = (desired+cur_gpu)/2;
			}
			break;
		case TRANSITION2:
			if( desired <= cur_gpu){
				//state = STATE.TRANSITION3;
				state = STATE.TRANSITION1;
				bIsGpuAvailable = false;
			}else if( util_avg > upper_threshold ){
				state = STATE.FULL;
				bIsGpuAvailable = false;
			}else if ( cur_gpu <= desired - 2 ){
				state = STATE.UNDER;
				desired = max_gpu;
				bIsGpuAvailable = true;
			}
			break;
		case UNDER:
			if( util_avg > upper_threshold ){
				state = STATE.FULL;
				bIsGpuAvailable = false;
			}else if( cur_gpu == 0 ){
				state = STATE.EMPTY;
				desired = max_gpu;
				bIsGpuAvailable = true;
			}
			break;
		}
		
		if( cur_gpu == 0 ){
			state = STATE.EMPTY;
			desired = max_gpu;
			bIsGpuAvailable = true;
		}
		
		if( bIsGpuAvailable && isNewMapTask ){
			processCount = processCount + 1;
		}
		
		return bIsGpuAvailable;
	}
	
	public boolean FSM_maxStart(int cur_gpu,int max_gpu,boolean isNewMapTask) {
		processCount = cur_gpu;
		this.max_gpu = max_gpu;

		switch (state) {
		case EMPTY:
			if( cur_gpu >= max_gpu ){
				desired = max_gpu;
				//bNumOverMaxGpu = true;
				state = STATE.TRANSITION1;
				bIsGpuAvailable = false;
			}
			break;
		case TRANSITION1:
			if( util_avg > upper_threshold ){
			//if( cur_gpu >= desired ){
				state = STATE.FULL;
				bIsGpuAvailable = false;
			}else if ( cur_gpu <= desired - 2 ){
				state = STATE.UNDER;
				desired = max_gpu;
				bIsGpuAvailable = true;
			}
			break;
		case FULL:
			if( util_avg <= upper_threshold ){
				state = STATE.TRANSITION2;
				bIsGpuAvailable = true;
				desired = cur_gpu+2>max_gpu ? max_gpu : cur_gpu+2;
				//X = cur_gpu;
				X = (desired+cur_gpu)/2;
			}
			break;
		case TRANSITION2:
			if( desired <= cur_gpu){
				//state = STATE.TRANSITION3;
				state = STATE.TRANSITION1;
				bIsGpuAvailable = false;
			}else if( util_avg > upper_threshold ){
				state = STATE.FULL;
				bIsGpuAvailable = false;
			}else if ( cur_gpu <= desired - 2 ){
				state = STATE.UNDER;
				desired = max_gpu;
				bIsGpuAvailable = true;
			}
			break;
		case UNDER:
			if( util_avg > upper_threshold ){
				state = STATE.FULL;
				bIsGpuAvailable = false;
			}else if( cur_gpu == 0 ){
				state = STATE.EMPTY;
				desired = max_gpu;
				bIsGpuAvailable = true;
			}
			break;
		}
		
		if( cur_gpu == 0 ){
			state = STATE.EMPTY;
			desired = max_gpu;
			bIsGpuAvailable = true;
		}
		
		if( bIsGpuAvailable && isNewMapTask ){
			processCount = processCount + 1;
		}
		
		return bIsGpuAvailable;
	}
	
	public boolean isGpuAvailable(int cur_gpu,int max_gpu,boolean isNewMapTask) {
		if( max_start_mode )
			return FSM_maxStart(cur_gpu, max_gpu, isNewMapTask);
		else
			return FSM_conservative(cur_gpu, max_gpu, isNewMapTask);
	}

	enum STATE {
		//INITIAL, TRANSITION1, TRANSITION2, INTERMEDIATE, FULL, EXTRA
		EMPTY, TRANSITION1, FULL,TRANSITION2, TRANSITION3,UNDER,
		WAIT,INCRE
	}

	public void run() {
		int[] utilizations = new int[16];
		int utilization_sum = 0;
		int[] memories = new int[16];
		int memories_sum = 0;
		int idxCount = 0;
		boolean bIsUpper = false;

		for (int i = 0; i < utilizations.length; i++) {
			utilizations[i] = 0;
			memories[i] = 0;
		}

		String err = initnvml();
		String mes = "<Java>GPUMonitor : err code from JNI : " + err;
		sendMsg(mes);

		long t_pre_send_msg = 0;
		while (bRunning) {
			long t_total_elapsed = System.nanoTime();
			long t_elapsed = System.nanoTime();
			int value = getState();
			t_elapsed = System.nanoTime() - t_elapsed;
			int utilization = (value & 0x000000FF);
			int memory = (value & 0x0000FF00) >> 8;
			utilization_sum += utilization - utilizations[idxCount];
			utilizations[idxCount] = utilization;
			idxCount = (idxCount + 1) & 0xF;
			
			util_avg = (float) utilization_sum / 16.0f;
			t_total_elapsed = System.nanoTime() - t_total_elapsed;
			isGpuAvailable( processCount, this.max_gpu , false);
			try {
				Thread.sleep(66);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if( !bUse_debug_listener ) continue;
			long t_cur = System.currentTimeMillis();
			if (t_cur - t_pre_send_msg > 1000) {
				t_pre_send_msg = t_cur;
				mes = "";
				if( max_start_mode ){
					mes += " ---- scheduler mode : max_start\n";
				}else{
					mes += " ---- scheduler mode : conservative \n";
				}
				mes += "util!!! : ";
				for (int util : utilizations) {
					mes += "" + util + ",";
				}
				mes += "sum : " + utilization_sum + "\n";
				mes += "value : " + value + "\n";
				mes += "state : " + state + "\n";
				mes += "isAvail : " + bIsGpuAvailable + "\n";
				mes += "desired : " + desired + "\n";
				mes += "X : " + X + "\n";
				mes += "processCount : " + processCount + "\n";
				mes += "utilization : " + utilization + "\n";
				mes += "util_avg : " + util_avg + "\n";
				mes += "elapsed : " + (long) (t_elapsed / 1000) + " us \n";
				mes += "total elapsed : " + (long)(t_total_elapsed/1000) + " us\n";
				if( state != STATE.EMPTY ){	
					sendMsg(mes);
					print_state = false;
				}
				else{
					if( !print_state ){
						print_state = true;
						sendMsg(mes);
					}
				}
			}
		}
	}

	public native String initnvml();

	public native int getState();

	static {
		System.loadLibrary("JNI_GPUMonitor");
	}

	public void setNumMinGpuMapTask(int num_min_gpu_maptask) {
		// TODO Auto-generated method stub
		this.num_min_gpu_maptask = num_min_gpu_maptask;
	}
}
