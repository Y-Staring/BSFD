#include <fstream>
#include <iostream>
#include <string>
#include <sstream>
#include <chrono>
#include <omp.h>
#include <mpi.h>
#include "algorithmsClassic/TANE.hpp"
#include "algorithmsClassic/DFD.hpp"
#include <vector>
#include <chrono>
#include <thread>

#include <condition_variable>
#include <functional>
#include <unistd.h>

int main() {
    //initialize the MPI environment
    MPI_Init(nullptr, nullptr);
    int rank, size;
    //rank of processes
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    //number of processes
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    std::cout<<"Hello FROM RANK"<<rank<<"of"<<size<<std::endl;
    int num_files = 13;
    std::string algo;
    algo = "TANE";
    //algo = "DFD";
    std::cout<<"num_files:"<<num_files<<std::endl;
    std::string line;
    std::string filename;
    
    
    
    std::vector<std::string> filenames;
  
    std::ifstream file("input.txt");
    if (!file) {
        char cwd[1024];
        if (getcwd(cwd, sizeof(cwd)) != NULL) {
            std::cout << "Current working directory: " << cwd << std::endl;
        }
        std::cout << "Unable to open file!" << std::endl;
        return 1;
    }
    std::getline(file, line);
    std::stringstream ss(line);
    while (ss >> filename) {
        filenames.push_back(filename);
    }

    int files_per_process = num_files / size;
    int start_file = rank * files_per_process;
    int end_file = (rank == size - 1) ? num_files : start_file + files_per_process;
    //int start_file = rank * num_files / size;
    //int end_file = (rank + 1) * num_files / size;

    double start = omp_get_wtime();
    omp_set_num_threads(13);
#pragma omp parallel for schedule(dynamic, 1)
    for (int i = start_file; i < end_file; ++i) {
        //std::string fullPath = "./datasets/subsets/subadult_allnode_aic/" + filenames[i]; 
        //std::string fullPath = "./datasets/subsets/subhosp_allnode_aic/"+filenames[i];
        //std::string fullPath = "./datasets/subsets/subamazon_allnode_aic/"+filenames[i]; 
        //std::string fullPath = "./datasets/subsets/substu_allnode_aic/"+filenames[i];
        //std::string fullPath = "./datasets/subsets/subflight_allnode_aic/"+filenames[i];    
        //std::string fullPath = "./datasets/subsets/substat_allnode_aic/"+filenames[i];       




        //scalability
        //std::string fullPath = "./datasets/subsets/subhosp25/" +filenames[i];
        //std::string fullPath = "./datasets/subsets/subhosp50/" +filenames[i];
        //std::string fullPath = "./datasets/subsets/subhosp75/" +filenames[i];

        //std::string fullPath = "./datasets/subsets/subhosp25allnode/" +filenames[i];
        //std::string fullPath = "./datasets/subsets/subhosp50allnode/" +filenames[i];
        //std::string fullPath = "./datasets/subsets/subhosp75allnode/" +filenames[i];



        //std::string fullPath = "./datasets/subsets/subchild25/" +filenames[i];
        //std::string fullPath = "./datasets/subsets/subchild50/" +filenames[i];
        //std::string fullPath = "./datasets/subsets/subchild75/" +filenames[i];
        //std::string fullPath = "./datasets/subsets/subchild100/" +filenames[i];

        //std::string fullPath = "./datasets/subsets/subchild25allnode/" +filenames[i];
        //std::string fullPath = "./datasets/subsets/subchild50allnode/" +filenames[i];
        //std::string fullPath = "./datasets/subsets/subchild75allnode/" +filenames[i];
        //std::string fullPath = "./datasets/subsets/subchild100allnode/" +filenames[i];


        //std::string fullPath = "./datasets/subsets/submildew23/" +filenames[i];
        //std::string fullPath = "./datasets/subsets/submildew31/" +filenames[i];
        //std::string fullPath = "./datasets/subsets/submildew27/" +filenames[i];
        //std::string fullPath = "./datasets/subsets/submildew35/" +filenames[i];

        //std::string fullPath = "./datasets/subsets/submildew23allnode/" +filenames[i];
        //std::string fullPath = "./datasets/subsets/submildew27allnode/" +filenames[i];
        //std::string fullPath = "./datasets/subsets/submildew31allnode/" +filenames[i];
        //std::string fullPath = "./datasets/subsets/submildew35allnode/" +filenames[i];



        //std::string fullPath = "./datasets/subsets/subflight8/" +filenames[i];
        //std::string fullPath = "./datasets/subsets/subflight12/" +filenames[i];
        //std::string fullPath = "./datasets/subsets/subflight16/" +filenames[i];

        //std::string fullPath = "./datasets/subsets/subflight8allnode/" +filenames[i];
        //std::string fullPath = "./datasets/subsets/subflight12allnode/" +filenames[i];
        //std::string fullPath = "./datasets/subsets/subflight16allnode/" +filenames[i];



        //std::string fullPath = "./datasets/subsets/subalarmallnode/" +filenames[i];
        //13
        //std::string fullPath = "./datasets/subsets/subalarm22/" +filenames[i];
        //22
        //std::string fullPath = "./datasets/subsets/subalarm27/" +filenames[i];
        //15
        //std::string fullPath = "./datasets/subsets/subalarm32/" +filenames[i];
        //17
        //std::string fullPath = "./datasets/subsets/subalarm37/" +filenames[i];


        //14
        //std::string fullPath = "./datasets/subsets/subamazon/" +filenames[i];
        //13
        std::string fullPath = "./datasets/subsets/substatalog/" +filenames[i];
        //3
        //std::string fullPath = "./datasets/subsets/subfood/" +filenames[i];

        //5
        //std::string fullPath = "./datasets/subsets/subhosp/" +filenames[i];
        //4
        //std::string fullPath = "./datasets/subsets/subhospaic/" +filenames[i];

        //10
        //std::string fullPath = "./datasets/subsets/subadult/" +filenames[i];

        //19
        //std::string fullPath = "./datasets/subsets/substu/" +filenames[i];
        //6
        //std::string fullPath = "./datasets/subsets/subflight/" +filenames[i];
        std::cout<<fullPath<<std::endl;
        std::ifstream ifile(fullPath);
        if (!ifile) {
             std::cout << "Subtable path error, can not open!" << std::endl;
             exit(1);
        }
        char cwd[1024];
        if (getcwd(cwd, sizeof(cwd)) != NULL) {
            std::cout << "Current working directory: " << cwd << std::endl;
        }
        if (algo == "DFD"){
            //DFD
            std::set<std::string> allFD;
            DFD dfd(fullPath);
            dfd.extraction();
            dfd.get_deps(allFD);
            std::cout<<"FD size:"<<allFD.size()<<std::endl;
            for (auto& x : allFD) {
                std::cout<<x<<std::endl;
             }
            
        }
        if (algo == "TANE"){
            std::set<std::pair<std::string, std::string>> allFD;
            TANE t;
            t.read_data(fullPath);
            t.run();
            t.get_deps(allFD);
            std::cout<<"FD size:"<<allFD.size()<<std::endl;
            for (auto& fd : allFD) {
                //TANE
                std::string result; 
                result = fd.first + " -> " + fd.second;
                std::cout<<result<<std::endl;
                //os << result << std::endl;
            }
        }
        
        

        printf("I am process %d, thread %d, i=%d\n", rank, omp_get_thread_num(), i);
        
        std::cout << "Current working file: " << filenames[i] << std::endl;
        }
    
    //os.close();
    MPI_Barrier(MPI_COMM_WORLD);
    double end = omp_get_wtime();

    double total_time;
    MPI_Reduce(&end, &total_time, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);

    if (rank == 0) {
        std::cout << "Total execution time: " << total_time - start << "s" << std::endl;
        //std::set<std::pair<std::string, std::string>> globalFD;

        //for (auto &fd : allFD) {
        //    globalFD.insert(fd);
        //}
        //std::cout<<"FDs number:"<<allFD.size()<<std::endl;

    }

     MPI_Finalize();
    return 0;
}

//int main() {
//    std::string line;
//    std::string filename;
//    std::vector<std::string> filenames;
//    std::vector<std::string> parameters;
//    int parm1, parm2;
//    int count = 0;
//    std::ifstream file("../input.txt");
//    if (!file) {
//        char cwd[1024];
//        if (getcwd(cwd, sizeof(cwd)) != NULL) {
//            std::cout << "Current working directory: " << cwd << std::endl;
//        }
//        std::cout << "Unable to open file!" << std::endl;
//        return 1;
//    }
//    std::getline(file, line);
//    std::stringstream ss(line);
//    while (ss >> filename) {
//        filenames.push_back(filename);
//    }
//    while (std::getline(file, line)) {
//        parameters.push_back(line);
//    }
//    for (const auto &param: parameters) {
//        std::cout << param << std::endl;
//        if (count == 0) {
//            parm1 = std::stoi(param);
//        } else if (count == 1) {
//            parm2 = std::stoi(param);
//        }
//        count++;
//    }
//        omp_set_num_threads(10);
//        double start = omp_get_wtime();
//#pragma omp parallel for schedule (dynamic, 1)
//        for (int i = 0; i < 7; ++i) {
//            std::string fullPath = "../subsets/" + filenames[i];
//            std::ifstream ifile(fullPath);
//            Database db = TabularDatabase::fromFile(ifile, ',');
//            CCFDMiner m(db, parm1, parm2);
//            printf("I am thread %d,i=%d\n", omp_get_thread_num(), i);
//            m.run();
//        }
//        double end = omp_get_wtime();
//        std::cout << "The total execution time is:" << float(end - start) << "s" << std::endl;
//        return 0;
//}
