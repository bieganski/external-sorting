#include <seastar/core/app-template.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/file.hh>
#include "seastar/core/reactor.hh"
#include "seastar/core/sstring.hh"
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/memory.hh>

#include <iostream>
#include <algorithm>
#include <numeric>
#include <vector>



/*!
 *  \author    Mateusz Biegański
 *
 *  OPIS ROZWIĄZANIA:
 *
 *  Moja implementacja sortowania składa się z trzech faz:
 *  * wczytywanie i sortowanie mniejszych kawałków (PREPROCESSING)
 *  * sortowanie mniejszych bloków i zapisywanie ich do pomniejszych plików (MERGE)
 *  * scalanie wynikowych plików w jeden (OUTPUT MERGING)
 *
 *  Wejściowy plik wczytywany i sortowany jest w kawałkach na tyle dużych, żeby zmaksymalizować
 *  zużycie pamięci, nie przekraczając jednak wartości `RAM_AVAILABLE`. W następnej fazie pliki
 *  są ładowane z dysku, a następnie zapisywane w ostatecznej kolejności, uprzednio sortowane przez scalanie
 *  (k-way mergesort). W ostatniej fazie następuje scalanie posortowanych bloków w plkik wynikowy
 *  (wielowątkowe scalanie parami w czasie N*logN)
 *
 *
 *  ZAŁOŻENIA:
 *  * rekordy w pliku wejściowym nie są oddzielone żadnym znakiem, tj
 *  plik zawierający dwa rekordy ma dokładnie 8192 znaki.
 *  * plik wejściowy jest dobrze uformowany, tj. zawiera pełne rekordy.
 */

using namespace seastar;


static const std::string FNAME("./input.txt"); // TODO tu należy wpisać nazwę pliku wejściowego
static const std::string TMP_DIR("./tmp/");
static const std::string OUT_DIR("./out/");

using recordVector = std::vector<std::string>;
using numVector = std::vector<uint64_t>;

static const uint64_t RECORD_SIZE = 4096;


inline uint64_t RAM_AVAILABLE() {
    return memory::stats().total_memory();
}

inline uint64_t NUM_WORKERS() {
    return smp::count - 1;
}

inline sstring out_chunk_name(uint64_t num_chunk) {
    return sstring(OUT_DIR + std::string("chunk") + std::to_string(num_chunk));
}


/**
 * maximal number of record in one sorted chunk
 * written to disk as temporary file.
 */
inline uint64_t NUM_RECORDS_TO_READ_PER_THREAD() {
    uint64_t res = RAM_AVAILABLE() / NUM_WORKERS() / RECORD_SIZE;
    assert(res > 0 && "Not enough memory!");
    return res;
}



/**
 * delegates to each worker task of reading it's chunks, sorting them and saving
 * to disk
 */
future<> read_and_sort(std::vector<numVector>& workers_chunks_nums) {
    return parallel_for_each(workers_chunks_nums.begin(), workers_chunks_nums.end(), [](numVector chunks_nums) {
        std::cout << "thread: " << std::to_string(engine().cpu_id()) << "-- bede czytal" << chunks_nums << "\n";
//        return read_chunk_file(chunks_nums);
        return make_ready_future();
    });
}


future<> test() {
    std::cout << "TEST: " << smp::count;
    return make_ready_future();
}

/**
 * returns vector of size NUM_WORKERS(), where each worker is given
 * vector of indices (counting from 0) of records, from which they should start
 * reading chunks. for instance, given [3, 8] vector, thread reads two chunks, first
 * starting at 3 * RECORD_SIZE and second at 8 * RECORD_SIZE position, both containing
 * maximally NUM_RECORDS_TO_READ_PER_THREAD() of records (possibly except the end of the file)
 */
std::pair<uint64_t, std::vector<numVector>> chunks_to_be_read(uint64_t fsize) {
    uint64_t NUM_RECORDS = fsize / RECORD_SIZE;
    uint64_t per_each_thread = NUM_RECORDS / NUM_WORKERS();
    uint64_t remainder = NUM_RECORDS % NUM_WORKERS();

    std::vector<numVector> res(NUM_WORKERS(), numVector(per_each_thread));
    uint64_t num_chunk = 0;
    for (uint64_t i = 0; i < res.size(); i++) {
        std::iota(res[i].begin(), res[i].end(), num_chunk);
        num_chunk += per_each_thread;
    }
    for (uint64_t i = 0; i < remainder; i++) {
        res[i].emplace_back(num_chunk++);
    }
    return std::make_pair(num_chunk, res);
}


future<> external_sort() {
    static sstring fname(FNAME);
    static uint64_t i = 0;
    return open_file_dma(fname, open_flags::rw).then([](file f) {
        return f.size().then([f](uint64_t fsize) mutable {
            assert(fsize % RECORD_SIZE == 0 && "Malformed input file error! It's size must be divisible by record size");
            std::pair<uint64_t, std::vector<numVector>> res = chunks_to_be_read(fsize); // TODO zmien to plz
            uint64_t NUM_CHUNKS = res.first;
            std::vector<numVector> chunks_nums = res.second;
            return do_with(std::move(chunks_nums), [=](std::vector<numVector>& chunks_nums) mutable {
                return f.close().then([&chunks_nums]{
                    return read_and_sort(chunks_nums);
                });
            });
        });
    });
}


int compute(int argc, char **argv) {
    app_template app;
    try {
        app.run(argc, argv, external_sort);
    } catch (...) {
        std::cerr << "Couldn't start application: ";
        return 1;
    }
    return 0;
}




/**
 * TODO:
 * assert fsize % RECORD_SIZE == 0, gdzie fsize jest wejściowym plikiem
 */
int main(int argc, char **argv) {
    uint64_t MERGING_BLOCK_NUM_RECORDS = RAM_AVAILABLE() / RECORD_SIZE / 10;

    compute(argc, argv);
    std::cout << "Posortowany plik znajduje się w katalogu " << OUT_DIR << " pod nazwą chunk0" << std::endl;
    return 0;
}


