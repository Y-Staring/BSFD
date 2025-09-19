#include "DFD.hpp"
#include "TANE.hpp"
#include "FastAFD.hpp"
#include <ctime>
#include <iostream>
#include <string>
#include <fstream>
#include <filesystem>
#include <vector>
#include <unordered_map>
#include <iostream>
#include <unistd.h>
#include <set>

struct Job { std::string path; double supp; double conf; };


double run_TANE_job(std::string& path) {
    std::string path = "datasets/adult_clean.csv";
        
    std::cout<<"running TANE chorno! "<<path<<std::endl;
    std::string path_copy = path;
    if (!path_copy.empty() && path_copy.back() == '/')
            path_copy.pop_back();

    // 找到最后一个 '/'
    size_t pos = path_copy.find_last_of("///");
    string last = (pos == string::npos) ? path_copy : path_copy.substr(pos + 1); // "subflight0dot1allnode"


    std::string output_path = std::string(" ") 
                + "TANEOriginal_" + last + ".txt";
    TANE t;
    clock_t start = clock();
    t.read_data(path);
    t.run();
    std::ofstream ofs(output_path);

    t.output(ofs);
    ofs.close();


    clock_t end = clock();
    double elapsed_time = ((double)(end - start) / CLOCKS_PER_SEC);
    std::cout<<elapsed_time<<"s"<<std::endl;
    return ((double)(end - start) / CLOCKS_PER_SEC);
}


double run_TANE() {
    std::string path = "datasets/adult_clean.csv";
    std::cout<<"running TANE chorno! "<<path<<std::endl;
    std::string path_copy = path;
    if (!path_copy.empty() && path_copy.back() == '/')
            path_copy.pop_back();

    size_t pos = path_copy.find_last_of("///");
    string last = (pos == string::npos) ? path_copy : path_copy.substr(pos + 1); 


    std::string output_path = std::string(" ") 
                + "TANEOriginal_" + last + ".txt";
    TANE t;
    clock_t start = clock();
    t.read_data(path);
    t.run();
    std::ofstream ofs(output_path);
    t.output(ofs);
    ofs.close();


    clock_t end = clock();
    double elapsed_time = ((double)(end - start) / CLOCKS_PER_SEC);
    std::cout<<elapsed_time<<"s"<<std::endl;
    return ((double)(end - start) / CLOCKS_PER_SEC);
}

double run_DFD_job(const std::string& path,bool test = false) {
    srand(time(0));
    if (test) {
        std::cout << "1: " << std::endl;
        DFD dfd("./servo.csv");
        dfd.extraction();
        assert(dfd.getFD().size() == 109);
        assert(dfd.getFD().at(48).at(0) == 5);
        assert(dfd.getFD().at(48).at(1) == 11);
        assert(dfd.getFD().at(48).at(2) == 12);
        return -1;
    } else {

        std::string path_copy = path;
        if (!path_copy.empty() && path_copy.back() == '/')
                path_copy.pop_back();

        // 找到最后一个 '/'
        size_t pos = path_copy.find_last_of("///");
        string last = (pos == string::npos) ? path_copy : path_copy.substr(pos + 1); // "subflight0dot1allnode"

   
        std::string output_path = std::string(" ") 
                    + "DFDOriginal_" + last + ".txt";
        DFD dfd(path);
 
        clock_t start = clock();
        dfd.extraction();
        std::ofstream os(output_path);
 
        dfd.output(os);
        os.close();
        clock_t end = clock();
        double elapse = ((double)(end - start) / CLOCKS_PER_SEC);
        std::cout << "Time elapsed: " << elapse << std::endl;
        return elapse;
    }
}

double run_DFD(bool test = false) {
    srand(time(0));
    if (test) {
        std::cout << "1: " << std::endl;
        DFD dfd("./servo.csv");
        dfd.extraction();
        assert(dfd.getFD().size() == 109);
        assert(dfd.getFD().at(48).at(0) == 5);
        assert(dfd.getFD().at(48).at(1) == 11);
        assert(dfd.getFD().at(48).at(2) == 12);
        return -1;
    } else {

        DFD dfd("datasets/adult_clean.csv");

        // std::cout<<"running DFD chorno  !"<<path<<std::endl;
        std::string output_path = " ";
        clock_t start = clock();
        dfd.extraction();
        std::ofstream os(output_path);

        dfd.output(os);
        os.close();
        clock_t end = clock();
        double elapse = ((double)(end - start) / CLOCKS_PER_SEC);
        std::cout << "Time elapsed: " << elapse << std::endl;
        return elapse;
    }
}

void run_FastAFD(bool test, const std::string& input_path) {
    srand(time(0));
    if (test) {
        std::cout << "1: " << std::endl;
        FastAFD fastfd("./test-hospital.csv");
        fastfd.extraction();
        assert(fastfd.getFD().size() == 109);
        assert(fastfd.getFD().at(48).at(0) == 5);
        assert(fastfd.getFD().at(48).at(1) == 11);
        assert(fastfd.getFD().at(48).at(2) == 12);
    }
    else {
        //std::string input_path = "/home/.../adult_clean.csv";
        auto pos1 = input_path.find_last_of("///");
        auto pos2 = input_path.find_last_of('.');
        std::string stem = input_path.substr(pos1 + 1, pos2 - pos1 - 1);
        std::string output_path = std::string(" ") 
                        + "fastfd_" + stem + ".txt";

        FastAFD fastfd(input_path);
        fastfd.extraction();
        std::ofstream os(output_path);
        fastfd.output(os);
    }
}


void test_TANE(int num_files,const std::string& path, bool part = false) {
    double total = 0;
    int loop = 3;
      if (part == true) {
        clock_t start = clock();
        // int num_files = 55;

        std::vector<std::string> filenames;
        std::set<std::pair<std::string, std::string>> allFD;
        std::string filename;
        std::string line;
        std::ifstream file("input.txt");
        if (!file) {
            char cwd[1024];
            if (getcwd(cwd, sizeof(cwd)) != NULL) {
                std::cout << "Current working directory: " << cwd << std::endl;
            }
            std::cout << "Unable to open file!" << std::endl;
            return;
        }
        std::getline(file, line);
        std::stringstream ss(line);
        while (ss >> filename) {
            filenames.push_back(filename);
        }
        std::string path_copy = path;
        if (!path_copy.empty() && path_copy.back() == '/')
                path_copy.pop_back();

            size_t pos = path_copy.find_last_of("///");
            string last = (pos == string::npos) ? path_copy : path_copy.substr(pos + 1); // "subflight0dot1allnode"

            const string prefix = "sub";
            if (last.rfind(prefix, 0) == 0) {
                last = last.substr(prefix.size());
            }
        std::string output_path = std::string("") 
                        + "TANE_bsfd_" + last + ".txt";

        std::cout<<output_path<<std::endl;
        std::ofstream depStream(output_path);

        for (int i = 0; i < num_files; ++i) {
            std::string filepath = path + filenames[i];
            std::cout << "Processing file: " << filepath << std::endl;
            TANE t;
            t.read_data(filepath);
            t.run();
            t.get_deps(allFD);
            // std::vector<std::pair<int, int>> uniqueFD(FD.begin(), FD.end()); // remove duplicates
        }
        for (auto& fd : allFD) {
            std::string result;
            
            result = fd.first + " -> " + fd.second;
            std::cout<<result<<std::endl;
            depStream << result << std::endl;
        }
        depStream.close();
        clock_t end = clock();
        double time = (end - start) / CLOCKS_PER_SEC;
        std::cout << "Time elapsed: " << time << std::endl;
    }
    else {
        run_TANE(); // warm up
        for (int i = 0; i < loop; ++i) {
            double elapse = run_TANE();
            std::cout << "Time elapsed: " << elapse << std::endl;
            total += elapse;
        }

        total /= loop;
        std::cout << "Average: " << total << std::endl;
    }
    //system("pause");
}

void test_DFD(int num_files,const std::string& path, bool part = false) {
    double total = 0;
    if (part == true) {
        clock_t start = clock();
        // int num_files = 55;
        std::vector<std::string> filenames;
        std::set<std::string> allFD;
        std::string filename;
        std::string line;
        std::ifstream file("input.txt");
        if (!file) {
            char cwd[1024];
            if (getcwd(cwd, sizeof(cwd)) != NULL) {
                std::cout << "Current working directory: " << cwd << std::endl;
            }
            std::cout << "Unable to open file!" << std::endl;
            return;
        }
        std::getline(file, line);
        std::stringstream ss(line);
        while (ss >> filename) {
            filenames.push_back(filename);
        }
        
        for (int i = 0; i < num_files; ++i) {
            std::cout<<i<<endl;
            std::string filepath = path + filenames[i];
            std::cout << "Processing file: " << filepath << std::endl;
            
            DFD dfd(filepath);
            dfd.extraction();
            dfd.get_deps(allFD);
            std::cout << "Current size of allFD after file " << filenames[i] << ": " << allFD.size() << std::endl;
        }
        std::string path_copy = path;
        if (!path_copy.empty() && path_copy.back() == '/')
                path_copy.pop_back();


        size_t pos = path_copy.find_last_of("///");
        string last = (pos == string::npos) ? path_copy : path_copy.substr(pos + 1); // "subflight0dot1allnode"

        // 去掉 "sub" 前缀
        const string prefix = "sub";
        if (last.rfind(prefix, 0) == 0) {
            last = last.substr(prefix.size());
        }
        std::string output_path = std::string("") 
                    + "DFD_bsfd_" + last + ".txt";
        std::ofstream os(output_path);
        try{
            std::cout << "Final size of allFD: " << allFD.size() << std::endl;
            
            std::cout<<output_path<<std::endl;
            for (auto& fd : allFD) {
                // std::string result;
                // for (auto x : fd) {
                //     result += std::to_string(x);
                //     result += " ";
                // }
                // result.replace(result.rfind(' '), 1, "");
                // result.replace(result.rfind(' '), 1, " -> ");
                os << fd << std::endl;
                
               
            }
        }catch(const std::exception& e){
                std::cerr <<e.what() << std::endl;
                return;
        }
        clock_t end = clock();
        double time = (end - start) / CLOCKS_PER_SEC;
        std::cout << "Time elapsed: " << time << std::endl;
        os.close();
        
    }
    else{
        int loop = 3;
        for (int i = 0; i < loop; ++i) {
            clock_t start, end;
            //start = clock();
            double diff = run_DFD(false);
            //end = clock();
            //double diff = (double)(end - start) / CLOCKS_PER_SEC;
            std::cout << "Time elapsed: " << diff << std::endl;
            total += diff;
            }
            total /= loop;
            std::cout << "Average: " << total << std::endl;
        }
    //system("pause");
}

void test_FastAFD(bool part, const std::string& path) {
    double total = 0;
    if (part == true) {
        int num_files = 9;
        std::string path = "datasets/subsets/sub70hepar";
        std::vector<std::string> filenames;
        std::set<std::vector<int>> FD;
        std::string filename;
        std::string line;
        std::ifstream file("input.txt");
        if (!file) {
            char cwd[1024];
            if (getcwd(cwd, sizeof(cwd)) != NULL) {
                std::cout << "Current working directory: " << cwd << std::endl;
            }
            std::cout << "Unable to open file!" << std::endl;
            return;
        }
        std::getline(file, line);
        std::stringstream ss(line);
        while (ss >> filename) {
            filenames.push_back(filename);
        }
        
        for (int i = 0; i < num_files; ++i) {
            std::string filepath = path + filenames[i];
            FastAFD fastfd(filepath);
            fastfd.extraction();
            std::vector<std::vector<int>> tmpFD = fastfd.getFD();
            FD.insert(tmpFD.begin(), tmpFD.end());
        }
        std::ofstream os("./FastAFD_part.txt");
        for (auto& fd : FD) {
            std::string result;
            for (auto x : fd) {
                result += std::to_string(x);
                result += " ";
            }
            result.replace(result.rfind(' '), 1, "");
            result.replace(result.rfind(' '), 1, " -> ");
            os << result << std::endl;
        }
    }
    else {
        int loop = 3;
        for (int i = 0; i < loop; ++i) {
            clock_t start, end;
            start = clock();
            run_FastAFD(false,path);
            end = clock();
            double diff = (double)(end - start) / CLOCKS_PER_SEC;
            std::cout << "Time elapsed: " << diff << std::endl;
            total += diff;
        }
        total /= loop;
        std::cout << "Average: " << total << std::endl;
    }
    system("pause");
}


static std::string fmt(double x) {
    std::string s = std::to_string(x);
    while (!s.empty() && s.back()=='0') s.pop_back();
    if (!s.empty() && s.back()=='.') s.pop_back();
    for (auto &ch : s) if (ch=='.') ch = 'p';
    if (s.empty()) s = "0";
    return s;
}

static std::string get_stem(const std::string& path) {
    size_t slash = path.find_last_of("///");
    size_t start = (slash == std::string::npos) ? 0 : slash + 1;
    size_t dot   = path.find_last_of('.');
    if (dot == std::string::npos || dot < start) dot = path.size();
    return path.substr(start, dot - start);
}


void run_FastAFD_job(const Job& job, const std::string& out_dir) {
    //std::string stem = get_stem(job.path);
    //std::string out_name = "fastafd_" + stem + "_s" + fmt(job.supp) + "_c" + fmt(job.conf) + ".txt";
    //std::string out_path = out_dir + (out_dir.back()=='/' ? "" : "/") + out_name;
    std::string path_copy = job.path;
    if (!path_copy.empty() && path_copy.back() == '/')
            path_copy.pop_back();

    size_t pos = path_copy.find_last_of("///");
    string last = (pos == string::npos) ? path_copy : path_copy.substr(pos + 1); // "subflight0dot1allnode"

    const string prefix = "sub";
    if (last.rfind(prefix, 0) == 0) {
        last = last.substr(prefix.size());
    }
    std::string out_path = out_dir
                + last + ".txt";
    std::cout << "[FastAFD] Running " << job.path
              << " supp=" << job.supp << " conf=" << job.conf
              << " -> " << out_path << std::endl;

    clock_t start, end;
    start = clock();
    FastAFD fastfd(job.path, job.supp, job.conf); 
    fastfd.extraction();
    std::ofstream os(out_path);
    fastfd.output(os);
    os.close();
    end = clock();
    double diff = (double)(end - start) / CLOCKS_PER_SEC;

    std::cout << "[FastAFD] Done, time=" << diff << "s/n";
}

int main() {
    vector<string> paths = {

        //"datasets/synthetic_70_hepar.csv",
    //    "datasets/Amazon-sale-report-clean.csv",
    //    "datasets/statlog_german_credit_data.csv",
    //    "datasets/hospital_clean.csv",
    //    "datasets/studentfull_26.csv",
    //    "datasets/flights_20_500k_clean.csv",
    //"/datasets/scalability/sampled_hospital_0.25.csv",
    //"/datasets/scalability/sampled_hospital_0.50.csv",
    //"/datasets/scalability/sampled_hospital_0.75.csv",
    //"/datasets/scalability/synthetic_child_25W.csv",
    //"/datasets/scalability/synthetic_child_50W.csv",
    //"/datasets/scalability/synthetic_child_75W.csv",
    //"/datasets/scalability/synthetic_child_100W.csv"
    ///datasets/scalability/flight_8_clean.csv
    ///datasets/scalability/flight_12_clean.csv
    ///datasets/scalability/flight_16_clean.csv
       //"exps/robustness/noisedata/amazon_noised_0.01.csv",
       //"exps/robustness/noisedata/amazon_noised_0.1.csv", 
       //"exps/robustness/noisedata/amazon_noised_0.2.csv", 
       //"exps/robustness/noisedata/amazon_noised_0.3.csv", 
    };

    vector<string> subpaths = {
        
        "datasets/subsets/subadult0dot15/"
        
    };

    vector<int> numOfFiles = {4};


    for(int i=0; i < subpaths.size();i++){
        std::cout<<"running"<<subpaths[i]<<std::endl;
        //run_DFD_job(paths[i]);
        //run_TANE_job(paths[i]);
        test_DFD(numOfFiles[i],subpaths[i],true);
        //test_TANE(numOfFiles[i],subpaths[i],true);
    }



    std::vector<Job> jobs = {
        // ================== row scalability ==================
        //{"datasets/scalability/synthetic_child_100W.csv", 0.1, 0.9},
        //{"datasets/scalability/synthetic_child_75W.csv",  0.1, 0.9},
        //{"datasets/scalability/synthetic_child_50W.csv",  0.1, 0.9},
        //{"datasets/scalability/synthetic_child_25W.csv",  0.1, 0.9},
        //{"datasets/hospital_clean.csv", 0.1, 0.9},
        //{"datasets/scalability/sampled_hospital_0.75.csv",0.1, 0.9},
        //{"datasets/scalability/sampled_hospital_0.50.csv",0.1, 0.9},
        //{"datasets/scalability/sampled_hospital_0.25.csv",0.1, 0.9},

        // ================== col scalability ==================
        //{"datasets/flights_20_500k_clean.csv", 0.1, 0.9},
        //{"datasets/scalability/flight_16_clean.csv", 0.1, 0.9},
        //{"datasets/scalability/flight_12_clean.csv", 0.1, 0.9},
        //{"datasets/scalability/flight_8_clean.csv",  0.1, 0.9},

  
       //{"/datasets/scalability/synthetic_hepar_58.csv", 0.1, 0.9},
       //{"/datasets/scalability/synthetic_hepar_62.csv", 0.1, 0.9},
       //{"/datasets/scalability/synthetic_hepar_66.csv", 0.1, 0.9},

        // ================== support scalability (student) ==================
        //{"datasets/studentfull_26.csv", 0.05, 0.9},
        //{"datasets/studentfull_26.csv", 0.10, 0.9},
        //{"datasets/studentfull_26.csv", 0.15, 0.9},
        //{"datasets/studentfull_26.csv", 0.20, 0.9},

        // ================== support scalability (alarm) ==================
        //{"datasets/synthetic_alarm.csv", 0.05, 0.9},
        //{"datasets/synthetic_alarm.csv", 0.10, 0.9},
        //{"datasets/synthetic_alarm.csv", 0.15, 0.9},
        //{"datasets/synthetic_alarm.csv", 0.20, 0.9},

        // ================== confidence scalability (amazon) ==================
        //{"datasets/Amazon-sale-report-clean.csv", 0.1, 0.85},
        //{"datasets/Amazon-sale-report-clean.csv", 0.1, 0.90},
        //{"datasets/Amazon-sale-report-clean.csv", 0.1, 0.95},
        //{"datasets/Amazon-sale-report-clean.csv", 0.1, 1.00},

        //// ================== confidence scalability (flight) ==================
        //{"datasets/flights_20_500k_clean.csv", 0.1, 0.85},
        //{"datasets/flights_20_500k_clean.csv", 0.1, 0.90},
        //{"datasets/flights_20_500k_clean.csv", 0.1, 0.95},
        //{"datasets/flights_20_500k_clean.csv", 0.1, 1.00},
    };

    // define by yourself
    std::string out_dir = "";

    //for (const auto& job : jobs) {
    //    run_FastAFD_job(job, out_dir);
    //}

    //for(int i=0; i < paths.size();i++){
    //    std::cout<<"running"<<paths[i]<<std::endl;
    //    run_FastAFD(false,paths[i]);
    //}
    
    // test_DFD(true);
    //test_TANE(true);
    //run_FastAFD();
    //run_DFD(); 
    //run_TANE();
    //double elapse = run_TANE();
    //std::cout << "Time elapsed: " << elapse << std::endl;
    return 0;
}  



