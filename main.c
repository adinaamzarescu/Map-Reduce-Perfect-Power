#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include<pthread.h>

struct mapData {
    char *fileName;
    int finalCount;
 };

void reduceArray(int *nums, int finalCount)
{
    //Print array
    printf("\n");
    for(int i=0;i<finalCount;i++)
    {
        if(nums[i] == -1)
        {
            continue;;
        }
        printf("%d, ",nums[i]);
    }
    
    
    //Calculating count of unique nos in array
    int count = 0;
    
    for(int i =0;i<finalCount;i++)
    {
       if(nums[i] == -1)
       {
           continue;;
       }
       else{
           count++;
       }
        int num = nums[i];
        //Replacing all other values than num as -1
        for(int j =0;j<finalCount;j++)
        {
            if(nums[j]==-1 || i == j)
            {
                continue;
            }
            if(num == nums[j])
            {
                nums[j] = -1;
            }
            
        }
       
    }
    
    printf("\nResult-%d\n",count);
}


int getNoOfFiles(const char *fileName)
{
    int value = 0;
    
    FILE *testfile;
   
    testfile = fopen(fileName,"r");
    
    char firstLine[100];
    
    if(testfile == NULL)
    {
        printf("File not found ~%s...!\n",fileName);
        return -1;
    }
    
    int count = 0;
    
    //Reading test file for only 1st line and getting no of file links and saving file links in array
    
    while((fgets(firstLine,100,testfile)!=NULL) && count<1)
    {
        if(count<1)
        {
            value = atoi(firstLine);
        }
       
        count++;
    }
    
    fclose(testfile);
    
    return value;
}

int checkPerfectPower(int num)
{
    //Checking if the number is perfect power
       
    for(long i = 2;i*i <=num ;i++)
    {
        if(i == num)
        {
            return 1;
        }
        long j = i;
       
        while(j<= num)
        {
            j *= i;
            if(j == num)
            {
                return 1;
            }
        }
        
    }
    
    return 0;
    
}


//int getNumberArrayFromFile(char fileName[100], int finalCount){
//    FILE *inputfile;
////    char *fileName;
////    fileName = ((struct mapData*)input)->fileName;
////    int finalCount =((struct mapData*)input)->finalCount;
//
//    //Getting no of nos in the file
//    int fileCount = getNoOfFiles(fileName);
//
//    printf("For file %s, fileCount is %d\n",fileName,fileCount);
//
//    return 0;
//}

void* getNumberArrayFromFile(void * input){
    FILE *inputfile;
    char *fileName;
    fileName = ((struct mapData*)input)->fileName;
    int finalCount =((struct mapData*)input)->finalCount;

   // printf("For file %s, fileCount is %d\n",fileName,finalCount);

    int *nums;
    nums = malloc(sizeof(int) * finalCount);
    inputfile = fopen(fileName,"r");

    char line[100];

    if(inputfile == NULL)
    {
        printf("File %s not found...terminating!\n",fileName);

    }
    else{

        //Reading from line by line from file and storing links in an int array
        int count = 0,start = 0;
        
        while(fgets(line,100,inputfile)!= NULL && count<=finalCount)
        {
            line[strlen(line)-1] = '\0';
           //Skipping the first line
            if(start==0)
            {
                start++;

            }
            else{
                nums[count] = atoi(line);
                count++;
            }

        }
        //Opening file to read nos line by line


        //Testing array
        for(int j =1;j<count;j++)
        {
            if(nums[j] == -1)
            {
                continue;
            }
            int powerCheck = checkPerfectPower(nums[j]);

            if(!powerCheck)
            {
                //Setting all same nos to -1

                for(int k = 1;k<count;k++)
                {
                    if(k==j)
                    {
                        continue;
                    }
                    if(nums[k] == nums[j])
                    {
                        nums[k] = -1; //setting the number as -1 in case its not a perfect power
                    }
                }

                nums[j]  = -1;

            }
            
        }
        
        reduceArray(nums,finalCount);
        
        free(nums);
       
        fclose(inputfile);
    }

    return 0;
}


int mapProcess(char **mapArray,int finalCount)
{
//    //  Testing array
//                     for(int i =0;i<finalCount;i++)
//                     {
//                         printf("File link readytoprocess - %s\n",mapArray[i]);
//                     }
    //Declaring struct for processing files
    struct mapData* data[finalCount];
    pthread_t processThread[finalCount];
    
    //Processing File read by looping the array for files
    
    for(int i = 0;i<finalCount;i++)
    {
        //Saving parameters to Struct
        data[i] = malloc(sizeof(mapArray[i])*finalCount+sizeof(finalCount)+20);
        
        if(data[i] == NULL)
        {
            printf("\nCould not allocate memory\n");
        }
       
    
        data[i]->fileName =  mapArray[i];
        int count = getNoOfFiles(mapArray[i]);
        data[i]->finalCount = count ;
   //     getNumberArrayFromFile(mapArray[i],finalCount);

        //Sending processes to threads

        pthread_create(&processThread[i],NULL,getNumberArrayFromFile,(void *)data[i]);
        pthread_join(processThread[i],NULL);
        free(data[i]);
    }
    
    
    return 0;
}


void allocateMapsAndReducers(char fileLinks[][100],int noOfFiles,int noOfMappers,int noOfReducers){
    
    //Setting files per map based on number of files
    int filesPerMap = noOfFiles <= noOfMappers ? 1 : noOfFiles/noOfMappers;
    
//        //Testing array
//        for(int i =0;i<noOfFiles;i++)
//        {
//            printf("File links  - %s\n",fileLinks[i]);
//        }
    
  
    
    //Defining arrays to be passed to Maps for processing
    int fileCount = 0,processCount = 1,finalFileCount = 0;;
    char *mapArray[filesPerMap];
    int readyToProcess = 0;
    
    for(int i = 0;i < noOfFiles;i++)
    {
      if(fileCount < filesPerMap)
        {
            mapArray[fileCount] = malloc(sizeof(fileLinks[i])+1);
            mapArray[fileCount] = fileLinks[i];
            
            
            if(i == noOfFiles)
            {
                readyToProcess = 1;
                finalFileCount = fileCount;
            }
            fileCount++;
        }
        else{
          
            if(processCount>=noOfMappers)
            {
                while(i<= noOfFiles)
                {
                    mapArray[fileCount] = malloc(sizeof(fileLinks[i])+1);
                    mapArray[fileCount] = fileLinks[i];
                    fileCount++;
                    i++;
                }
                readyToProcess = 1;
                finalFileCount = fileCount-1;
           
            }
            else{
                i--;
                readyToProcess = 1;
                finalFileCount = fileCount;
                fileCount = 0;
                processCount++;
            }
        }
        
        if(readyToProcess)
        {
          //  Testing array
//            printf("\n\n");
//                 for(int i =0;i<finalFileCount;i++)
//                 {
//                     printf("File link readytoprocess - %s\n",mapArray[i]);
//                 }
                    
            mapProcess(mapArray,finalFileCount);
            readyToProcess = 0;
            
        }
    }
    
}


int readFile(int argc, const char** argv,int noOfFiles)
{
    int noOfMappers = atoi(argv[1]);
    int noOfReducers = atoi(argv[2]);
    
  printf("noOfMappers - %d,noOfReducers - %d, file - %d\n ",noOfMappers,noOfReducers,noOfFiles);
    
    char fileLinks[noOfFiles][100];
    
    FILE *testfile;
   
    testfile = fopen(argv[3],"r");
    
    char line[100] = "";
    if(testfile == NULL)
    {
        printf("File not found...terminating!\n");
        return -1;
    }
    
    int count = 0,start = 0;
    
    //Reding from line by line from file and storing links in an array fileLinks
    
    while(fgets(line,100,testfile)!=NULL && count<noOfFiles)
    {
       // printf("line is %s",line);
        if(start == 0)
        {
            start++;
            
        }
        else{
            if(count == noOfFiles-1)
            {
                line[strlen(line)] = '\0';
            }
            else{
                line[strlen(line)-1] = '\0';
            }
            
            strcpy(fileLinks[count],line);
            count++;
        }
       
    }
      
    fclose(testfile);
    
    //Testing array
//    for(int i =0;i<count;i++)
//    {
//        printf("File link - %s\n",fileLinks[i]);
//    }
     
    //Allocating Maps & reducers & Sending for further actions
    
   allocateMapsAndReducers(fileLinks,noOfFiles,noOfMappers,noOfReducers);
    
       
    return 0;
}

int main(int argc, const char * argv[]) {
  
    //Reading file and all the arguments
    int noOfFiles = getNoOfFiles(argv[3]);
    readFile(argc, argv,noOfFiles);
    
    
    return 0;
}
