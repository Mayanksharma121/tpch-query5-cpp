#include "query5.hpp"
#include <iostream>
#include <chrono> // Time measurement ke liye zaroori
#include <iomanip> // Output formatting ke liye

// Baaki headers query5.hpp mein already shamil hain

int main(int argc, char* argv[]) {
    // 1. Argument Parsing
    std::string r_name, start_date, end_date, table_path, result_path;
    int num_threads = 1; // Default
    
    if (!parseArgs(argc, argv, r_name, start_date, end_date, num_threads, table_path, result_path)) {
        std::cerr << "Usage: " << argv[0] << " --r_name <REGION> --start_date <YYYY-MM-DD> --end_date <YYYY-MM-DD> --threads <N> --table_path <PATH/TO/TBL> --result_path <OUTPUT.TXT>" << std::endl;
        return 1;
    }

    // 2. Data Containers (Correct Data Structures use karein)
    CustomerMap customer_data;
    OrdersMap orders_data;
    LineitemVector lineitem_data;
    SupplierMap supplier_data;
    NationMap nation_data;
    RegionMap region_data;
    std::map<std::string, double> results;

    std::cout << "--- Starting TPC-H Query 5 Execution ---" << std::endl;

    // --- 3. Step: Data Loading ---
    auto start_load = std::chrono::high_resolution_clock::now();
    std::cout << "1. Loading data from: " << table_path << std::endl;
    
    // readTPCHData ko updated signature ke saath call karein
    if (!readTPCHData(table_path, customer_data, orders_data, lineitem_data, supplier_data, nation_data, region_data)) {
        std::cerr << "Failed to read TPCH data." << std::endl;
        return 1;
    }
    auto end_load = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> load_duration = end_load - start_load;


    // --- 4. Step: Query Execution ---
    auto start_exec = std::chrono::high_resolution_clock::now();
    std::cout << "2. Executing Query 5 (Threads: " << num_threads << ", Region: " << r_name << ")..." << std::endl;
    
    // executeQuery5 ko updated signature ke saath call karein
    if (!executeQuery5(r_name, start_date, end_date, num_threads, customer_data, orders_data, lineitem_data, supplier_data, nation_data, region_data, results)) {
        std::cerr << "Failed to execute TPCH Query 5." << std::endl;
        return 1;
    }
    auto end_exec = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> exec_duration = end_exec - start_exec;


    // --- 5. Step: Output Results ---
    if (!outputResults(result_path, results)) {
        std::cerr << "Failed to output results." << std::endl;
        return 1;
    }

    // --- 6. Step: Final Runtime Reporting ---
    std::cout << "\n=======================================================" << std::endl;
    std::cout << "✅ TPC-H Query 5 Completed Successfully" << std::endl;
    std::cout << std::fixed << std::setprecision(4);
    std::cout << "   Total Data Load Time:    " << load_duration.count() << " seconds" << std::endl;
    std::cout << "   Query Execution Time (" << num_threads << "T): " << exec_duration.count() << " seconds" << std::endl;
    std::cout << "=======================================================" << std::endl;
    
    return 0;
}