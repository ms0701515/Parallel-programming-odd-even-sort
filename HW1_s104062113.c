#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mpi.h>
#define CHANNEL0 0
#define CHANNEL1 1
#define bool char
#define false 0
#define true 1
int rank, proc_num, size;
float *data, *recv, *temp;
bool isSorted=false;
void read_write(int data_num, char *fileName, float *data, int * count, int r_w){
    //printf("File Name: %s Count: %d\n", fileName, *count);
    MPI_File fh;
    MPI_Status status;
    MPI_File_open(MPI_COMM_WORLD, fileName, r_w, MPI_INFO_NULL, &fh);
    MPI_File_set_view(fh, sizeof(float)*size*rank, MPI_FLOAT, MPI_FLOAT, "native", MPI_INFO_NULL);
    //printf("In Read Testcase, rank=%d, size=%d\n", rank, size);
    if(r_w == MPI_MODE_RDONLY){
        MPI_File_read_all(fh, data, size, MPI_FLOAT, &status);
        MPI_Get_count(&status, MPI_FLOAT, count);        
        //printf("Get count: rank=%d, count=%d\n", rank, *count);
    }else if(r_w == (MPI_MODE_WRONLY | MPI_MODE_CREATE)){
        MPI_File_write_all(fh, data, *count, MPI_FLOAT, &status);
        //printf("Write file works\n");
   }
   MPI_File_close(&fh);
   //printf("Done read_write function\n");
}
int cmp(const void *a, const void *b){
    float c = *(float*)a;
    float d = *(float*)b;
    if(c>d)return 1;
    else if(c==d) return 0;
    else  return -1;
}
void swap(float *a, float *b){
    float temp;
    temp=*a;
    *a=*b;
    *b=temp;
}
bool isOdd(int rank){
    if(rank%2)
         return true;
    else 
         return false;
}
void merge(float *buffer, int left_size, float *recv, int recv_size, bool phase){
    int i=0, j=0, k=0, mem_size;
    float *mem_ptr;
    while(i<left_size || j<recv_size){
        if(i>=left_size)
            temp[k++]=recv[j++];
        else if(j>=recv_size)
            temp[k++]=buffer[i++];
        else
            temp[k++]=(buffer[i]<=recv[j])?buffer[i++]:recv[j++];
    }
    mem_size=sizeof(float)*left_size;
    mem_ptr=(phase==0)? temp: temp+recv_size;
    if(memcmp(buffer, mem_ptr, mem_size)!=0)
        isSorted=false;
    memcpy(buffer, mem_ptr, mem_size);
}
int commu(int proc, float *target, int send_size, float *buffer, int recv_size, int channel){
    if(proc<0 || proc>=proc_num || rank>=proc_num)
        return -1;
    //printf("In commu: proc=%d, send_size=%d, recv_size=%d",proc, send_size, recv_size);
    MPI_Status status;
    MPI_Sendrecv(target, send_size, MPI_FLOAT, proc, channel,
                 buffer, recv_size, MPI_FLOAT, proc, !channel, MPI_COMM_WORLD, &status);
    int cnt;
    MPI_Get_count(&status, MPI_FLOAT, &cnt);
    return cnt;
}
void left2Right(int proc, int count){
    int cnt = commu(proc, data, count, recv, size, CHANNEL1);
    if(cnt<0)
        return;
    merge(data, count, recv, cnt, 0);
}
void right2Left(int proc, int count){
    int cnt = commu(proc, data, count, recv, size, CHANNEL0);
    if(cnt<0)
        return;
    else
    merge(data, count, recv, cnt, 1);
}
int main(int argc, char *argv[])
{
    //Initialize MPI World
    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &proc_num);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    int data_num = atoi(argv[1]);
    //printf("data_num: %d\n", data_num);
    proc_num = (proc_num > data_num)? data_num : proc_num;
    size = (data_num + proc_num - 1)/proc_num;
    data = (float*)malloc(sizeof(float)*size);
    recv = (float*)malloc(sizeof(float)*size);
    temp = (float*)malloc(sizeof(float)*size*2);
    //printf("size = %d\n", size); 
    //Read Testcase
    int count;
    read_write(data_num, argv[2], data, &count, MPI_MODE_RDONLY);
    //printf("Done Read Testcase, rank=%d, size=%d, proc_num=%d, count=%d\n", rank, size, proc_num, count);
    //If there is only one data, do nothing
    if(proc_num<=1)
         isSorted=true;
    
    //Sort before communicate
    if(rank*size<data_num)
        qsort(data, count, sizeof(float), cmp); 
    //printf("Before Change\n");
    int x=0;
    //for(x=0; x<count; x++){
    //    printf("%f ", data[x]);
    //}
    //Exchange until all data is sorted
    MPI_Barrier(MPI_COMM_WORLD);
    while(isSorted==false){
        //Even phase
        //printf("In Change\n");
        isSorted=true;  
        if(isOdd(rank)==false)
	    left2Right(rank+1, count);
        else
            right2Left(rank-1, count);
        MPI_Barrier(MPI_COMM_WORLD);
        
        //Odd phase
        if(isOdd(rank))
            left2Right(rank+1, count);
        else
            right2Left(rank-1, count);
        MPI_Barrier(MPI_COMM_WORLD);

        bool t=isSorted;
        MPI_Allreduce(&t, &isSorted, 1, MPI_CHAR, MPI_BAND, MPI_COMM_WORLD);
    }
    //printf("rank=%d, count=%d\n", rank, count);
    //for(x=0; x<count; x++)
       // printf("%f ", data[x]);
    //printf("\n"); 
    MPI_Barrier(MPI_COMM_WORLD);
    int c;
    c=(rank*size>=data_num)?0:count;
    read_write(data_num, argv[3], data, &c, MPI_MODE_WRONLY|MPI_MODE_CREATE);
    free(data);
    free(recv);
    free(temp);
    //MPI_Barrier(MPI_COMM_WORLD);
    //printf("Done Before Finalize\n");
    MPI_Finalize();
    //printf("Done\n");
    return 0;
}

