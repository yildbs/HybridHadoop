JAVA_PATH=/usr/java/latest
JNI_INCLUDE:= -I$(JAVA_PATH)/include/ 
JNI_INCLUDE+= -I$(JAVA_PATH)/include/linux

all: dirs lib

dirs:
	mkdir -p obj

lib: obj/JNI_GPUMonitor.o
	g++ -o libJNI_GPUMonitor.so $< -shared -L./nvml/usr/src/gdk/nvml/lib -lnvidia-ml

obj/JNI_GPUMonitor.o: JNI_GPUMonitor.cpp
	g++ -c $< -o $@ -fPIC $(JNI_INCLUDE)

clean:
	rm *.o
	rm *.so
