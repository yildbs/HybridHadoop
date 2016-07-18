/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.ContainerManagerImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ResourceLocalizationService;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * The launcher for the containers. This service should be started only after
 * the {@link ResourceLocalizationService} is started as it depends on creation
 * of system directories on the local file-system.
 * 
 */
public class ContainersLauncher extends AbstractService implements
		EventHandler<ContainersLauncherEvent> {



	private static final Log LOG = LogFactory.getLog(ContainersLauncher.class);

	private final Context context;
	private final ContainerExecutor exec;
	private final Dispatcher dispatcher;
	private final ContainerManagerImpl containerManager;

	private LocalDirsHandlerService dirsHandler;
	@VisibleForTesting
	public ExecutorService containerLauncher = Executors
			.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat(
					"ContainersLauncher #%d").build());
	@VisibleForTesting
	public final Map<ContainerId, ContainerLaunch> running = Collections
			.synchronizedMap(new HashMap<ContainerId, ContainerLaunch>());

	public ContainersLauncher(Context context, Dispatcher dispatcher,
			ContainerExecutor exec, LocalDirsHandlerService dirsHandler,
			ContainerManagerImpl containerManager) {
		super("containers-launcher");
		this.exec = exec;
		this.context = context;
		this.dispatcher = dispatcher;
		this.dirsHandler = dirsHandler;
		this.containerManager = containerManager;

	}

	protected boolean isGpuAvailable(int cur_gpu,int max_gpu,boolean isNewMapTask){
		boolean ret = false;
		if(bUse_dynamic_scheduler)
			ret = gpumonitor.isGpuAvailable(cur_gpu,max_gpu,isNewMapTask);
		else{
			if (running_num_of_gpu_yarnchild < max_num_of_gpu_yarnchild) 
				ret = true;
			else
				ret = false;
		}
		return ret;
	}
	
	@Override
	protected void serviceInit(Configuration conf) throws Exception {
		try {
			// TODO Is this required?
			FileContext.getLocalFSFileContext(conf);
		} catch (UnsupportedFileSystemException e) {
			throw new YarnRuntimeException(
					"Failed to start ContainersLauncher", e);
		}

		this.upper_threshold = conf.getFloat("myconf.upper.threshold",96.0f );
		this.gpu_threshold = conf.getFloat("myconf.gpu.threshold",0.7f );
		this.cpugpu_proportion = conf.getFloat("myconf.cpugpu.proportion",1f );
		this.num_of_nodes = conf.getInt("myconf.num.nodes",1 );
		this.debug_listener_address = conf.get("myconf.debug.listener.address", "node01");
		this.bUse_debug_listener = conf.getBoolean("myconf.debug.listener.use", false);
		this.bUse_dynamic_scheduler = conf.getBoolean("myconf.use.dynamic.scheduler", false);

		gpumonitor = new GPUMonitor(debug_listener_address,bUse_debug_listener , upper_threshold);
		if( this.bUse_dynamic_scheduler ){
			gpumonitorthread = new Thread( gpumonitor);
			gpumonitorthread.start();
		}
		
		boolean max_start_mode = conf.getBoolean("myconf.use.maxstart.scheduler", true);;
		if(this.bUse_dynamic_scheduler ){
			gpumonitor.SetMaxStartMode(max_start_mode);
		}

		String slbp = conf.get("myconf.load.balancing.policy","threshold");
		if( slbp.startsWith("threshold") ){
			this.lbp = LOAD_BALANCING_POLICY.threshold;
		}else if( slbp.startsWith("auto") ){
			this.lbp = LOAD_BALANCING_POLICY.auto;
		}
		
		max_num_of_container = conf.getInt("yarn.nodemanager.resource.memory-mb", -1);
		if( max_num_of_container != -1 ) max_num_of_container = max_num_of_container / 2048;
		this.max_num_of_gpu_yarnchild = conf.getInt("myconf.num.gpu.yarnchild",4)>max_num_of_container
				?max_num_of_container:conf.getInt("myconf.num.gpu.yarnchild",4);
		this.num_min_gpu_maptask = conf.getInt("myconf.num.min.gpu.yarnchild",1);
		if( bUse_debug_listener ){
			gpumonitor.setNumMinGpuMapTask(num_min_gpu_maptask);
		}
				
		if( this.max_num_of_gpu_yarnchild == 0 ) bOnlyCPU = true;
		
		max_num_of_cpu_yarnchild = max_num_of_container - max_num_of_gpu_yarnchild;
		
		int cur_gpu = running_num_of_gpu_yarnchild;
		int max_container = max_num_of_container;
		int max_gpu = max_num_of_gpu_yarnchild;
		int max_cpu = max_num_of_container - max_num_of_gpu_yarnchild;
		String stat = "NodeManager launched\n";
		stat += "max_gpu : " + max_gpu +"\n";
		stat += "min_gpu : " + num_min_gpu_maptask +"\n";
		stat += "max_cpu : " + max_cpu +"\n";
		stat += "max_container  : " + max_container +"\n";
		stat += "cpugpu_proportion : + " + cpugpu_proportion + "\n";
		stat += "debug_listener_address : " + debug_listener_address + "\n";
		stat += "bUse_debug_listener : " + bUse_debug_listener + "\n";
		stat += "bUse_dynamic_scheduler : " + bUse_dynamic_scheduler + "\n";
		if( bUse_debug_listener ){
			if( max_start_mode ){
				stat += " ---- scheduler mode : max_start\n";
			}else{
				stat += " ---- scheduler mode : conservative \n";
			}
		}
		stat += "upper_threshold : " + upper_threshold +"\n";
		stat += "gpu_threshold : " + gpu_threshold + "\n";
		stat += "load balancing policy : " + lbp + "\n";
		gpumonitor.sendMsg(stat);

		super.serviceInit(conf);
	}

	@Override
	protected void serviceStop() throws Exception {
		containerLauncher.shutdownNow();
		super.serviceStop();
	}

	String debug_listener_address = "";
	boolean bUse_debug_listener = false;
	boolean bOnlyCPU = false;
	boolean bMustGPU = false;
	public float cpugpu_proportion = 1;
	public int num_of_nodes = 1;
	public int count_cpu_yarnchild = 0;
	public int count_gpu_yarnchild = 0;
	public int max_num_of_container;
	public int max_num_of_cpu_yarnchild;
	public int max_num_of_gpu_yarnchild;
	public int num_min_gpu_maptask;
	public int running_num_of_gpu_yarnchild = 0;
	public int running_num_of_all_yarnchild = 0;
	public int running_num_of_mr = 0;
	public boolean bUse_dynamic_scheduler = false;
	public float upper_threshold = 90;
	public float gpu_threshold = 0.7f;
	Thread gpumonitorthread;
	GPUMonitor gpumonitor;
	LOAD_BALANCING_POLICY lbp;
	enum LOAD_BALANCING_POLICY{
		auto,threshold;
	}
	
	@Override
	public void handle(ContainersLauncherEvent event) {
		// TODO: ContainersLauncher launches containers one by one!!
		int num_tasks = -1;
		int cur_tasks = -1;
		
		Container container = event.getContainer();
		ContainerId containerId = container.getContainerId();
		
		List<String> commands = container.getLaunchContext().getCommands();
		boolean isMRAppMaster = false;
		for (String command : commands) {
			isMRAppMaster = command.contains("MRAppMaster");
		}

		switch (event.getType()) {
		case LAUNCH_CONTAINER:
			Application app = context.getApplications().get(
					containerId.getApplicationAttemptId().getApplicationId());

			
			ContainerLaunch launch = new ContainerLaunch(context, getConfig(),
					dispatcher, exec, app, event.getContainer(), dirsHandler,
					containerManager);
			
			if (isMRAppMaster == false) {
				running_num_of_all_yarnchild++;
				int pos_insert = 0;
				String mycmd = commands.get(0);
				for (int index = 0; index < mycmd.length(); index++) {
					if (mycmd.charAt(index + 0) == 'j'
							&& mycmd.charAt(index + 1) == 'a'
							&& mycmd.charAt(index + 2) == 'v'
							&& mycmd.charAt(index + 3) == 'a') {
						pos_insert = index + 4;
						break;
					}
				}
				if (pos_insert != 0) {
					String s_num_tasks = (String)mycmd.subSequence(mycmd.indexOf("num.map.tasks=")+14, mycmd.indexOf("num.map.tasks=")+20);
					s_num_tasks = s_num_tasks.substring(0, s_num_tasks.indexOf(" "));
					num_tasks = Integer.parseInt(s_num_tasks);
					
					String s_cur_tasks = (String)mycmd.subSequence(mycmd.indexOf("attempt_")+29, mycmd.indexOf("attempt_")+35);
					cur_tasks = Integer.parseInt(s_cur_tasks);

					int N = num_tasks / num_of_nodes;
					if( bOnlyCPU ){
						mycmd = mycmd.substring(0, pos_insert)
								+ " -DuseGPU=false"
								+ mycmd.substring(pos_insert, mycmd.length());
						count_cpu_yarnchild++;
					}
					else if ( isGpuAvailable(running_num_of_gpu_yarnchild,max_num_of_gpu_yarnchild,true) ) {
						mycmd = mycmd.substring(0, pos_insert)
								+ " -DuseGPU=true"
								+ mycmd.substring(pos_insert, mycmd.length());
						running_num_of_gpu_yarnchild++;
						count_gpu_yarnchild++;
						isGpuAvailable(running_num_of_gpu_yarnchild,max_num_of_gpu_yarnchild,false);
					}else if( bMustGPU ){
						mycmd = mycmd.substring(0, pos_insert)
								+ " -DuseGPU=true"
								+ mycmd.substring(pos_insert, mycmd.length());
						running_num_of_gpu_yarnchild++;
						isGpuAvailable(running_num_of_gpu_yarnchild,max_num_of_gpu_yarnchild,false);
						count_gpu_yarnchild++;
					}else if( ((lbp == LOAD_BALANCING_POLICY.auto) && (cur_tasks > N/4)) ){
						float X = gpumonitor.X;
						float Y = max_num_of_container - X - running_num_of_mr;
						int b = count_cpu_yarnchild;
						float P = cpugpu_proportion;
						
						float f_n_gpu_iteration = (N-b)/P/X;
						float f_n_cpu_iteration = (b+1) / Y;
						
						int n_gpu_iteration = (int) Math.ceil( (N-b)/P/X );
						int n_cpu_iteration = (int) Math.ceil( (b+1) / Y );
						
						if( n_gpu_iteration <= n_cpu_iteration ){
							mycmd = mycmd.substring(0, pos_insert)
									+ " -DuseGPU=true"
									+ mycmd.substring(pos_insert, mycmd.length());
							running_num_of_gpu_yarnchild++;
							isGpuAvailable(running_num_of_gpu_yarnchild,max_num_of_gpu_yarnchild,false);
							count_gpu_yarnchild++;
							
							bMustGPU = true;
						}else{
							mycmd = mycmd.substring(0, pos_insert)
									+ " -DuseGPU=false"
									+ mycmd.substring(pos_insert, mycmd.length());
							count_cpu_yarnchild++;
			
						}
					}else{
						if( cur_tasks > (int)((float)num_tasks*gpu_threshold)){
							mycmd = mycmd.substring(0, pos_insert)
									+ " -DuseGPU=true"
									+ mycmd.substring(pos_insert, mycmd.length());
							running_num_of_gpu_yarnchild++;
							isGpuAvailable(running_num_of_gpu_yarnchild,max_num_of_gpu_yarnchild,false);
							count_gpu_yarnchild++;
						}
						else {
							mycmd = mycmd.substring(0, pos_insert)
									+ " -DuseGPU=false"
									+ mycmd.substring(pos_insert, mycmd.length());
							count_cpu_yarnchild++;
						}
					}
					commands.remove(0);
					commands.add(0, mycmd);
				}
			}else{
				running_num_of_mr ++;
				max_num_of_cpu_yarnchild= max_num_of_container - max_num_of_gpu_yarnchild - running_num_of_mr;
			}

			containerLauncher.submit(launch);
			running.put(containerId, launch);
			break;
		case RECOVER_CONTAINER:
			if( isMRAppMaster == false ){
				running_num_of_all_yarnchild++;
			}else{
				running_num_of_mr ++;
			}
			
			app = context.getApplications().get(
					containerId.getApplicationAttemptId().getApplicationId());
			launch = new RecoveredContainerLaunch(context, getConfig(),
					dispatcher, exec, app, event.getContainer(), dirsHandler,
					containerManager);
			containerLauncher.submit(launch);
			running.put(containerId, launch);
			break;
		case CLEANUP_CONTAINER:
			if( isMRAppMaster == false ){
				running_num_of_all_yarnchild--;
					
				if (commands.toString().contains("useGPU=true")) {
					running_num_of_gpu_yarnchild--;
					isGpuAvailable(running_num_of_gpu_yarnchild,max_num_of_gpu_yarnchild,false);
				}else{
					if( bUse_debug_listener ){
						gpumonitor.CpuMapTaskIsOver();
					}
				}
				if( running_num_of_all_yarnchild == 0 ){
					running_num_of_gpu_yarnchild = 0;
					
					bMustGPU = false;
					count_cpu_yarnchild = 0;
					count_gpu_yarnchild = 0;
					isGpuAvailable(running_num_of_gpu_yarnchild,max_num_of_gpu_yarnchild,false );
				}
			}else{
				running_num_of_mr --;
				max_num_of_cpu_yarnchild = max_num_of_container - max_num_of_gpu_yarnchild;
			}

			ContainerLaunch launcher = running.remove(containerId);
			if (launcher == null) {
				// Container not launched. So nothing needs to be done.
				return;
			}
			// Cleanup a container whether it is running/killed/completed, so
			// that
			// no sub-processes are alive.
			try {
				launcher.cleanupContainer();
			} catch (IOException e) {
				LOG.warn("Got exception while cleaning container "
						+ containerId + ". Ignoring.");
			}
			break;
		}
	}
}
