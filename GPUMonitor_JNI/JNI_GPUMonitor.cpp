#include <stdio.h>
#include <unistd.h>
#include <iostream>
#include "sys/time.h"
#include "org_apache_hadoop_yarn_server_nodemanager_containermanager_launcher_GPUMonitor.h"
#include "nvml/usr/include/nvidia/gdk/nvml.h"

nvmlDevice_t device;
nvmlProcessInfo_t pinfos[10];

/*
 * Class:     org_apache_hadoop_yarn_server_nodemanager_containermanager_launcher_GPUMonitor
 * Method:    initnvml
 * Signature: ()Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_org_apache_hadoop_yarn_server_nodemanager_containermanager_launcher_GPUMonitor_initnvml
  (JNIEnv *env, jobject)
{
    nvmlReturn_t result;
    unsigned int device_count, i;
	char sentence[200];
	std::string err = "";

    result = nvmlInit();
    if (NVML_SUCCESS != result) { 
        printf("Failed to initialize NVML: %s\n", nvmlErrorString(result));
        sprintf(sentence, "Failed to initialize NVML: %s\n", nvmlErrorString(result));
		err.append( (std::string)sentence );
    }
	char name[NVML_DEVICE_NAME_BUFFER_SIZE];
	result = nvmlDeviceGetHandleByIndex(0, &device);
	if (NVML_SUCCESS != result) { 
		printf("Failed to get handle for device %i: %s\n", i, nvmlErrorString(result));
		sprintf(sentence,"Failed to get handle for device %i: %s\n", i, nvmlErrorString(result));
		err.append( (std::string)sentence );
		result = nvmlShutdown();
		return 0;
	}
	result = nvmlDeviceGetName(device, name, NVML_DEVICE_NAME_BUFFER_SIZE);
	if (NVML_SUCCESS != result) { 
		printf("Failed to get name of device %i: %s\n", i, nvmlErrorString(result));
		sprintf(sentence,"Failed to get name of device %i: %s\n", i, nvmlErrorString(result));
		err.append( (std::string)sentence );
		result = nvmlShutdown();
		return 0;
	}
	printf("Device : %s\n",name);
	sprintf(sentence,"Device : %s\n",name);
	err.append( (std::string)sentence );
	return env->NewStringUTF( err.c_str() );
}

/*
 * Class:     org_apache_hadoop_yarn_server_nodemanager_containermanager_launcher_GPUMonitor
 * Method:    getState
 * Signature: ()I
 */
JNIEXPORT jint JNICALL Java_org_apache_hadoop_yarn_server_nodemanager_containermanager_launcher_GPUMonitor_getState
  (JNIEnv *, jobject)
{
	unsigned int infoCount=-1;
    nvmlReturn_t result;
	result = nvmlDeviceGetComputeRunningProcesses(device , &infoCount, pinfos);
//	startTimer( &st_gur);
	nvmlUtilization_t utilization;
	nvmlDeviceGetUtilizationRates( device, &utilization);

	unsigned int return_value=0;
//            infoCount memory    gpu
//	0000 0000 0000 0000 0000 0000 0000 0000	
	return_value  = (utilization.gpu        ) & 0x000000FF;
	return_value |= (utilization.memory << 8) & 0x0000FF00;
	return_value |= (infoCount          <<16) & 0x00FF0000;
	return return_value;
}
