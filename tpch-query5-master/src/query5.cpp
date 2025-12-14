#include "query5.hpp"
#include <cmath>
#include <chrono>
#include <iostream>
#include <fstream>
#include <sstream>
#include <thread>
#include <mutex>
#include <algorithm>
#include <iomanip>
#include <stdexcept>
#include <vector>
#include <map>

// Global Mutex for thread-safe output (for result merging)
std::mutex cout_mutex;

// --- Helper Functions for Parsing ---

/**
 * @brief Reads a single TPC-H data file and parses its contents.
 *
 * @tparam T Type of the container (e.g., Map or Vector) to store the data.
 * @param path Base path to the table files.
 * @param container The container where parsed data will be stored.
 * @param filename Name of the table file (e.g., "nation").
 * @param parser Function pointer to the specific parsing logic for the table.
 * @return true if the file was successfully opened and processed, false otherwise.
 */
template <typename T>
bool readFile(const std::string &path, T &container, const std::string &filename, void (*parser)(std::stringstream &, T &))
{
    std::string full_path = path + "/" + filename + ".tbl";
    std::ifstream file(full_path);
    if (!file.is_open())
    {
        std::lock_guard<std::mutex> lock(cout_mutex);
        std::cerr << "Error: Could not open file: " << full_path << std::endl;
        return false;
    }

    std::string line;
    while (std::getline(file, line))
    {
        if (line.empty())
            continue;

        // Remove trailing '|' if present (TPC-H format)
        if (line.back() == '|')
        {
            line.pop_back();
        }

        try
        {
            std::stringstream ss(line);
            parser(ss, container);
        }
        catch (const std::exception &e)
        {
            std::lock_guard<std::mutex> lock(cout_mutex);
            std::cerr << "Parsing error in " << filename << ".tbl: " << e.what() << " on line: " << line.substr(0, 50) << "..." << std::endl;
            // Error in one line should not stop processing of other lines
        }
    }
    file.close();
    return true;
}

// --- Specific Parser Implementations ---

void parseNation(std::stringstream &ss, NationMap &nation_data)
{
    std::string segment;
    Nation n;
    std::getline(ss, segment, '|');
    n.N_NATIONKEY = std::stoi(segment);
    std::getline(ss, n.N_NAME, '|');
    std::getline(ss, segment, '|');
    n.N_REGIONKEY = std::stoi(segment);
    std::getline(ss, segment, '|'); // Ignore N_COMMENT
    nation_data[n.N_NATIONKEY] = n;
}

void parseRegion(std::stringstream &ss, RegionMap &region_data)
{
    std::string segment;
    Region r;
    std::getline(ss, segment, '|');
    r.R_REGIONKEY = std::stoi(segment);
    std::getline(ss, r.R_NAME, '|');
    std::getline(ss, segment, '|'); // Ignore R_COMMENT
    region_data[r.R_REGIONKEY] = r;
}

void parseCustomer(std::stringstream &ss, CustomerMap &customer_data)
{
    std::string segment;
    Customer c;
    std::getline(ss, segment, '|');
    c.C_CUSTKEY = std::stoi(segment);

    // Skip C_NAME, C_ADDRESS (2 fields)
    for (int i = 0; i < 2; ++i)
        std::getline(ss, segment, '|');

    // C_NATIONKEY
    std::getline(ss, segment, '|');
    c.C_NATIONKEY = std::stoi(segment);

    // Skip 4 remaining fields
    for (int i = 0; i < 4; ++i)
        std::getline(ss, segment, '|');
    customer_data[c.C_CUSTKEY] = c;
}

void parseSupplier(std::stringstream &ss, SupplierMap &supplier_data)
{
    std::string segment;
    Supplier s;
    std::getline(ss, segment, '|');
    s.S_SUPPKEY = std::stoi(segment);

    // Skip S_NAME, S_ADDRESS (2 fields)
    for (int i = 0; i < 2; ++i)
        std::getline(ss, segment, '|');

    // S_NATIONKEY
    std::getline(ss, segment, '|');
    s.S_NATIONKEY = std::stoi(segment);

    // Skip 4 remaining fields
    for (int i = 0; i < 4; ++i)
        std::getline(ss, segment, '|');
    supplier_data[s.S_SUPPKEY] = s;
}

void parseOrders(std::stringstream &ss, OrdersMap &orders_data)
{
    std::string segment;
    Orders o;
    std::getline(ss, segment, '|');
    o.O_ORDERKEY = std::stoi(segment);
    std::getline(ss, segment, '|');
    o.O_CUSTKEY = std::stoi(segment);

    // Skip O_ORDERSTATUS, O_TOTALPRICE
    std::getline(ss, segment, '|');
    std::getline(ss, segment, '|');

    // O_ORDERDATE
    std::getline(ss, o.O_ORDERDATE, '|');

    // Skip 5 remaining fields
    for (int i = 0; i < 5; ++i)
        std::getline(ss, segment, '|');
    orders_data[o.O_ORDERKEY] = o;
}

void parseLineitem(std::stringstream &ss, LineitemVector &lineitem_data)
{
    std::string segment;
    Lineitem l;

    std::getline(ss, segment, '|');
    l.L_ORDERKEY = std::stoi(segment);
    std::getline(ss, segment, '|'); // L_PARTKEY
    std::getline(ss, segment, '|');
    l.L_SUPPKEY = std::stoi(segment);
    std::getline(ss, segment, '|'); // L_LINENUMBER
    std::getline(ss, segment, '|'); // L_QUANTITY

    std::getline(ss, segment, '|');
    l.L_EXTENDEDPRICE = std::stod(segment);
    std::getline(ss, segment, '|');
    l.L_DISCOUNT = std::stod(segment);

    // Skip 8 remaining fields
    for (int i = 0; i < 8; ++i)
        std::getline(ss, segment, '|');

    lineitem_data.push_back(l);
}

// --- readTPCHData Function Implementation ---

bool readTPCHData(
    const std::string &table_path,
    CustomerMap &customer_data,
    OrdersMap &orders_data,
    LineitemVector &lineitem_data,
    SupplierMap &supplier_data,
    NationMap &nation_data,
    RegionMap &region_data)
{
    bool success = true;

    // Loading small tables first (Maps are generally faster for lookups)
    success &= readFile(table_path, region_data, "region", parseRegion);
    success &= readFile(table_path, nation_data, "nation", parseNation);
    success &= readFile(table_path, customer_data, "customer", parseCustomer);
    success &= readFile(table_path, supplier_data, "supplier", parseSupplier);
    success &= readFile(table_path, orders_data, "orders", parseOrders);

    // Lineitem is the largest table, load it last (Vector for sequential access)
    success &= readFile(table_path, lineitem_data, "lineitem", parseLineitem);

    if (success)
    {
        std::cout << "Data loading complete. Lineitems loaded: " << lineitem_data.size() << std::endl;
    }
    return success;
}

// --- Multithreading Execution Logic ---

/**
 * @brief Processes a chunk of the Lineitem table to calculate local revenue sums.
 *
 * This function performs the core join and filtering logic of Query 5 on its assigned data chunk.
 */
void process_lineitem_chunk(
    const std::string &r_name,
    const std::string &start_date,
    const std::string &end_date,
    const CustomerMap &customer_data,
    const OrdersMap &orders_data,
    const LineitemVector &lineitem_data,
    const SupplierMap &supplier_data,
    const NationMap &nation_data,
    const RegionMap &region_data,
    size_t start_index,
    size_t end_index,
    std::map<std::string, double> &local_results)
{
    // 1. Find the target R_REGIONKEY (e.g., R_NAME = 'ASIA')
    int target_region_key = -1;
    for (const auto &pair : region_data)
    {
        if (pair.second.R_NAME == r_name)
        {
            target_region_key = pair.first;
            break;
        }
    }
    if (target_region_key == -1)
        return;

    // 2. Iterate only on the assigned chunk of Lineitem (The main Join loop)
    for (size_t i = start_index; i < end_index; ++i)
    {
        const auto &L = lineitem_data[i];

        // 2.1. Find Order (O) and check Date Filter
        auto o_it = orders_data.find(L.L_ORDERKEY);
        if (o_it == orders_data.end())
            continue;
        const Orders &O = o_it->second;

        // Date Filter: o_orderdate >= start_date AND < end_date
        if (O.O_ORDERDATE < start_date || O.O_ORDERDATE >= end_date)
            continue;

        // 2.2. Find Customer (C) and Supplier (S)
        auto c_it = customer_data.find(O.O_CUSTKEY);
        auto s_it = supplier_data.find(L.L_SUPPKEY);

        if (c_it == customer_data.end() || s_it == supplier_data.end())
            continue;

        const Customer &C = c_it->second;
        const Supplier &S = s_it->second;

        // 2.3. Filter 1: Local Supplier Check (c_nationkey = s_nationkey)
        if (C.C_NATIONKEY != S.S_NATIONKEY)
            continue;

        // 2.4. Find Nation (N)
        auto n_it = nation_data.find(S.S_NATIONKEY);
        if (n_it == nation_data.end())
            continue;
        const Nation &N = n_it->second;

        // 2.5. Filter 2: Region Check (n_regionkey = target_region_key)
        if (N.N_REGIONKEY != target_region_key)
            continue;

        // 3. Calculate Revenue and Grouping (SUM)
        // sum(l_extendedprice * (1 - l_discount))
        double revenue = L.L_EXTENDEDPRICE * (1.0 - L.L_DISCOUNT);
        local_results[N.N_NAME] += revenue;
    }
}

// --- executeQuery5 Function Implementation ---

bool executeQuery5(
    const std::string &r_name,
    const std::string &start_date,
    const std::string &end_date,
    int num_threads,
    const CustomerMap &customer_data,
    const OrdersMap &orders_data,
    const LineitemVector &lineitem_data,
    const SupplierMap &supplier_data,
    const NationMap &nation_data,
    const RegionMap &region_data,
    std::map<std::string, double> &final_results)
{
    if (lineitem_data.empty())
    {
        std::cerr << "Error: Lineitem data is empty." << std::endl;
        return false;
    }

    size_t total_items = lineitem_data.size();
    size_t chunk_size = total_items / num_threads;
    std::vector<std::thread> threads;
    std::vector<std::map<std::string, double>> thread_results(num_threads);

    // 1. Launch Threads
    for (int i = 0; i < num_threads; ++i)
    {
        size_t start = i * chunk_size;
        // Last thread gets all remaining items
        size_t end = (i == num_threads - 1) ? total_items : (i + 1) * chunk_size;

        threads.emplace_back(process_lineitem_chunk,
                             r_name, start_date, end_date,
                             std::ref(customer_data),
                             std::ref(orders_data),
                             std::ref(lineitem_data),
                             std::ref(supplier_data),
                             std::ref(nation_data),
                             std::ref(region_data),
                             start, end,
                             std::ref(thread_results[i]));
    }

    // 2. Wait for all threads to finish (Join)
    for (auto &t : threads)
    {
        if (t.joinable())
        {
            t.join();
        }
    }

    // 3. Merge Results (Final Grouping)
    for (const auto &local_map : thread_results)
    {
        for (const auto &pair : local_map)
        {
            final_results[pair.first] += pair.second;
        }
    }

    return true;
}

// --- outputResults Function Implementation ---

bool outputResults(const std::string &result_path, const std::map<std::string, double> &results)
{
    // 1. Sort: Results by revenue (ORDER BY revenue desc)
    std::vector<std::pair<std::string, double>> sorted_results(results.begin(), results.end());

    std::sort(sorted_results.begin(), sorted_results.end(), [](const auto &a, const auto &b)
              {
                  return a.second > b.second; // Descending order
              });

    // 2. Write to File
    std::ofstream outfile(result_path);
    if (!outfile.is_open())
    {
        std::lock_guard<std::mutex> lock(cout_mutex);
        std::cerr << "Error: Could not open output file: " << result_path << std::endl;
        return false;
    }

    std::cout << "\n--- Final Query 5 Results (Written to " << result_path << ") ---" << std::endl;
    // Set precision for revenue output
    outfile << std::fixed << std::setprecision(2);
    std::cout << std::fixed << std::setprecision(2);

    outfile << "NATION_NAME|REVENUE" << "\n";
    for (const auto &pair : sorted_results)
    {
        outfile << pair.first << "|" << pair.second << "\n";
        std::cout << pair.first << "|" << pair.second << std::endl;
    }

    outfile.close();
    return true;
}

// --- parseArgs Function Implementation ---

bool parseArgs(
    int argc, 
    char *argv[],
    std::string &r_name,
    std::string &start_date,
    std::string &end_date,
    int &num_threads,
    std::string &table_path,
    std::string &result_path)
{
    num_threads = 1;

    for (int i = 1; i < argc; i++)
    {
        std::string arg = argv[i];

        if (arg == "--r_name" && i + 1 < argc)
        {
            r_name = argv[++i];
        }
        else if (arg == "--start_date" && i + 1 < argc)
        {
            start_date = argv[++i];
        }
        else if (arg == "--end_date" && i + 1 < argc)
        {
            end_date = argv[++i];
        }
        else if (arg == "--threads" && i + 1 < argc)
        {
            try
            {
                num_threads = std::stoi(argv[++i]);
            }
            catch (const std::exception &e)
            {
                std::cerr << "Invalid number of threads provided." << std::endl;
                return false;
            }
        }
        else if (arg == "--table_path" && i + 1 < argc)
        {
            table_path = argv[++i];
        }
        else if (arg == "--result_path" && i + 1 < argc)
        {
            result_path = argv[++i];
        }
        else
        {
            std::cerr << "Unknown or incomplete argument: " << arg << std::endl;
            return false;
        }
    }

    // Check if all required arguments are non-empty and threads > 0
    return !r_name.empty()
           && !start_date.empty()
           && !end_date.empty()
           && num_threads > 0
           && !table_path.empty()
           && !result_path.empty();
}