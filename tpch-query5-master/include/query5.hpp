#ifndef QUERY5_HPP
#define QUERY5_HPP

#include <string>
#include <vector>
#include <map>
#include <algorithm>
#include <iostream>
#include <stdexcept>

// --- 1. TPC-H Table Structs (Query 5 ke liye zaroori fields) ---

struct Region {
    int R_REGIONKEY;
    std::string R_NAME;
};

struct Nation {
    int N_NATIONKEY;
    std::string N_NAME;
    int N_REGIONKEY;
};

struct Customer {
    int C_CUSTKEY;
    int C_NATIONKEY;
    // Baki fields (C_NAME, C_ADDRESS, etc.) hum Query 5 ke liye ignore kar sakte hain
};

struct Supplier {
    int S_SUPPKEY;
    int S_NATIONKEY;
    // Baki fields (S_NAME, S_ADDRESS, etc.) hum Query 5 ke liye ignore kar sakte hain
};

struct Orders {
    int O_ORDERKEY;
    int O_CUSTKEY;
    std::string O_ORDERDATE; 
    // Baki fields ignore kar sakte hain
};

struct Lineitem {
    int L_ORDERKEY;
    int L_SUPPKEY;
    double L_EXTENDEDPRICE;
    double L_DISCOUNT;
    // Baki fields ignore kar sakte hain
};

// --- 2. Data Structure Type Aliases (Fast Joins ke liye Maps) ---

using RegionMap = std::map<int, Region>;
using NationMap = std::map<int, Nation>;
using CustomerMap = std::map<int, Customer>;
using SupplierMap = std::map<int, Supplier>;
using OrdersMap = std::map<int, Orders>; // O_ORDERKEY se lookup
using LineitemVector = std::vector<Lineitem>; // Isko iterate kiya jayega

// --- 3. Function Signatures (Declarations) ---

// Function to parse command line arguments
bool parseArgs(
    int argc, 
    char* argv[], 
    std::string& r_name, 
    std::string& start_date, 
    std::string& end_date, 
    int& num_threads, 
    std::string& table_path, 
    std::string& result_path
);

// Function to read TPCH data from the specified paths, using defined structures
bool readTPCHData(
    const std::string& table_path, 
    CustomerMap& customer_data, 
    OrdersMap& orders_data, 
    LineitemVector& lineitem_data, 
    SupplierMap& supplier_data, 
    NationMap& nation_data, 
    RegionMap& region_data
);

// Function to execute TPCH Query 5 using multithreading
bool executeQuery5(
    const std::string& r_name, 
    const std::string& start_date, 
    const std::string& end_date, 
    int num_threads, 
    const CustomerMap& customer_data, 
    const OrdersMap& orders_data, 
    const LineitemVector& lineitem_data, 
    const SupplierMap& supplier_data, 
    const NationMap& nation_data, 
    const RegionMap& region_data, 
    std::map<std::string, double>& results
);

// Function to output results to the specified path
bool outputResults(const std::string& result_path, const std::map<std::string, double>& results);

#endif // QUERY5_HPP