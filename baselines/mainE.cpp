#include "DFD.hpp"
#include "TANE.hpp"

#include <ctime>
#include <iostream>
#include <string>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <iostream>
#include <unistd.h>
#include <set>

double run_TANE()
{
    // std::string path = "./output.csv";

    // std::string path = "./datasets/scalability/sampled_hospital_0.25.csv";
    // std::string path = "./datasets/scalability/sampled_hospital_0.50.csv";
    // std::string path = "./datasets/scalability/sampled_hospital_0.75.csv";

    // std::string path = "./datasets/scalability/flight_8_clean.csv";
    // std::string path = "./datasets/scalability/flight_12_clean.csv";
    // std::string path = "./datasets/scalability/flight_16_clean.csv";

    // std::string path = "./datasets/scalability/synthetic_mildew_23.csv";
    // std::string path = "./datasets/scalability/synthetic_mildew_27.csv";
    // std::string path = "./datasets/scalability/synthetic_mildew_31.csv";
    // std::string path = "./datasets/scalability/synthetic_mildew_35.csv";

    // std:: string path ="./datasets/scalability/synthetic_child_100W.csv";
    // std::string path = "./datasets/scalability/synthetic_child_75W.csv";
    // std::string path = "./datasets/scalability/synthetic_child_50W.csv";
    // std::string path = "./datasets/scalability/synthetic_child_25W.csv";

    // std::string path = "./datasets/studentfull_26.csv";
    std::string path = "./datasets/flights_20_500k_clean.csv";
    // std::string path = "./datasets/adult_clean.csv";
    // std::string path = "./datasets/studentfull_26.csv";
    // std::string path = "./datasets/hospital_clean.csv";
    // std::string path = "./datasets/statlog_german_credit_data.csv";
    // std::string path = "./datasets/Amazon-sale-report-clean.csv";
    std::cout << "running TANE chorno! " << path << std::endl;
    TANE t;
    clock_t start = clock();
    t.read_data(path);
    t.run();
    std::ofstream ofs("./TANE_out_flight0.85_num.txt");
    t.output(ofs);
    ofs.close();

    clock_t end = clock();

    return ((double)(end - start) / CLOCKS_PER_SEC);
}

double run_DFD(bool test = false)
{
    srand(time(0));
    if (test)
    {
        std::cout << "1: " << std::endl;
        DFD dfd("./servo.csv");
        dfd.extraction();
        assert(dfd.getFD().size() == 109);
        assert(dfd.getFD().at(48).at(0) == 5);
        assert(dfd.getFD().at(48).at(1) == 11);
        assert(dfd.getFD().at(48).at(2) == 12);
        return -1;
    }
    else
    {
        // DFD dfd("./datasets/sampled_hospital_17_maxclass.csv");
        // DFD dfd("./datasets/sampled_adult_maxclass.csv");
        // DFD dfd("./datasets/sampled_census_clean.csv");
        // DFD dfd("./datasets/sampled_flight_maxclass.csv");
        // DFD dfd("./datasets/sampled_census_clean_no15.csv");
        // DFD dfd("./datasets/Amazon-sale-report-clean.csv");
        // DFD dfd("./datasets/flights_20_500k_clean.csv");
        // DFD dfd("./datasets/statlog_german_credit_data.csv");
        // DFD dfd("./datasets/adult_clean.csv");
        // DFD dfd("./datasets/hospital_clean.csv");
        // DFD dfd("./datasets/studentfull_26.csv");
        // std::string path = "./datasets/scalability/synthetic_alarm_22.csv";
        // std::string path = "./datasets/scalability/synthetic_alarm_27.csv";
        // std::string path = "./datasets/scalability/synthetic_alarm_32.csv";
        // std::string path = "./datasets/synthetic_alarm.csv";

        // std::string path = "./datasets/scalability/synthetic_mildew_23.csv";
        // std::string path = "./datasets/scalability/synthetic_mildew_27.csv";
        // std::string path = "./datasets/scalability/synthetic_mildew_31.csv";
        // std::string path = "./datasets/scalability/synthetic_mildew_35.csv";
        // std::string path  = "./datasets/studentfull_26.csv";
        // std::string path = "./datasets/scalability/synthetic_child_100W.csv";
        // std::string path = "./datasets/scalability/synthetic_alarm_22.csv";

        // std::string path = "./datasets/scalability/flight_8_clean.csv";
        // std::string path = "./datasets/scalability/flight_12_clean.csv";
        // std::string path = "./datasets/scalability/flight_16_clean.csv";

        // std::string path = "./datasets/scalability/sampled_hospital_0.25.csv";
        // std::string path = "./datasets/scalability/sampled_hospital_0.50.csv";
        // std::string path = "./datasets/scalability/sampled_hospital_0.75.csv";
        std::string path = "./datasets/flights_20_500k_clean.csv";
        DFD dfd(path);
        std::cout << "running DFD chorno  !" << path << std::endl;
        clock_t start = clock();
        dfd.extraction();
        std::ofstream os("./DFD_out_flight0.9.txt");
        dfd.output(os);
        os.close();
        clock_t end = clock();
        double elapse = ((double)(end - start) / CLOCKS_PER_SEC);
        std::cout << "Time elapsed: " << elapse << std::endl;
        return elapse;
    }
}


void test_TANE(bool part = false)
{
    double total = 0;
    int loop = 3;
    if (part == true)
    {
        clock_t start = clock();
        int num_files = 17;
        std::string path = "./datasets/subsets/subhosp_allnode_aic/";
        std::vector<std::string> filenames;
        std::set<std::pair<std::string, std::string>> allFD;
        std::string filename;
        std::string line;
        std::ifstream file("input.txt");
        if (!file)
        {
            char cwd[1024];
            if (getcwd(cwd, sizeof(cwd)) != NULL)
            {
                std::cout << "Current working directory: " << cwd << std::endl;
            }
            std::cout << "Unable to open file!" << std::endl;
            return;
        }
        std::getline(file, line);
        std::stringstream ss(line);
        while (ss >> filename)
        {
            filenames.push_back(filename);
        }

        std::ofstream depStream("TANE_part_hosp_allnode.txt");
        for (int i = 0; i < num_files; ++i)
        {
            std::string filepath = path + filenames[i];
            std::cout << "Processing file: " << filepath << std::endl;
            TANE t;
            t.read_data(filepath);
            t.run();
            t.get_deps(allFD);
            // std::vector<std::pair<int, int>> uniqueFD(FD.begin(), FD.end()); // remove duplicates
        }
        for (auto &fd : allFD)
        {
            std::string result;

            result = fd.first + " -> " + fd.second;
            depStream << result << std::endl;
        }
        depStream.close();
        clock_t end = clock();
        double time = (end - start) / CLOCKS_PER_SEC;
        std::cout << "Time elapsed: " << time << std::endl;
    }
    else
    {
        run_TANE(); // warm up
        for (int i = 0; i < loop; ++i)
        {
            double elapse = run_TANE();
            std::cout << "Time elapsed: " << elapse << std::endl;
            total += elapse;
        }

        total /= loop;
        std::cout << "Average: " << total << std::endl;
    }
    // system("pause");
}

void test_DFD(bool part = false)
{
    double total = 0;
    if (part == true)
    {
        clock_t start = clock();
        int num_files = 16;
        std::string path = "./datasets/subsets/subflight_allnode_aic/";
        std::vector<std::string> filenames;
        std::set<std::string> allFD;
        std::string filename;
        std::string line;
        std::ifstream file("input.txt");
        if (!file)
        {
            char cwd[1024];
            if (getcwd(cwd, sizeof(cwd)) != NULL)
            {
                std::cout << "Current working directory: " << cwd << std::endl;
            }
            std::cout << "Unable to open file!" << std::endl;
            return;
        }
        std::getline(file, line);
        std::stringstream ss(line);
        while (ss >> filename)
        {
            filenames.push_back(filename);
        }

        for (int i = 0; i < num_files; ++i)
        {
            std::cout << i << endl;
            std::string filepath = path + filenames[i];
            std::cout << "Processing file: " << filepath << std::endl;

            DFD dfd(filepath);
            dfd.extraction();
            dfd.get_deps(allFD);
            std::cout << "Current size of allFD after file " << filenames[i] << ": " << allFD.size() << std::endl;
        }

        try
        {
            std::cout << "Final size of allFD: " << allFD.size() << std::endl;
            std::ofstream os("./DFD_flight_allnode.txt");
            for (auto &fd : allFD)
            {
                // std::string result;
                // for (auto x : fd) {
                //     result += std::to_string(x);
                //     result += " ";
                // }
                // result.replace(result.rfind(' '), 1, "");
                // result.replace(result.rfind(' '), 1, " -> ");
                os << fd << std::endl;
                clock_t end = clock();
                double time = (end - start) / CLOCKS_PER_SEC;
                std::cout << "Time elapsed: " << time << std::endl;
            }
        }
        catch (const std::exception &e)
        {
            std::cerr << e.what() << std::endl;
            return;
        }
        // os.close();
    }
    else
    {
        int loop = 3;
        for (int i = 0; i < loop; ++i)
        {
            clock_t start, end;
            // start = clock();
            double diff = run_DFD(false);
            // end = clock();
            // double diff = (double)(end - start) / CLOCKS_PER_SEC;
            std::cout << "Time elapsed: " << diff << std::endl;
            total += diff;
        }
        total /= loop;
        std::cout << "Average: " << total << std::endl;
    }
    // system("pause");
}



int main()
{
    // test_DFD();
    test_TANE(true);
    // run_DFD();  //3565s_stu
    //    std::cout<<"heel"<<std::endl;
    // run_TANE();
    // double elapse = run_TANE();
    // std::cout << "Time elapsed: " << elapse << std::endl;
    return 0;
}
