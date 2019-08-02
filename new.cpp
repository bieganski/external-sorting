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
//    return memory::stats().total_memory();
    return RECORD_SIZE * 10;
}

inline uint64_t NUM_WORKERS() {
    return smp::count - 1;
}


inline sstring out_chunk_name(uint64_t num_chunk) {
    return sstring(OUT_DIR + std::string("chunk") + std::to_string(num_chunk));
}

inline sstring tmp_chunk_name(uint64_t num_chunk) {
    return sstring(TMP_DIR + std::string("chunk") + std::to_string(num_chunk));
}

inline uint64_t reading_pos(uint64_t num_chunk) {
    return num_chunk * RECORD_SIZE;
}


// możemy czytać bezpiecznie do `read_to - 1`
future<> read_records(file f, recordVector *save_to, uint64_t start_from, uint64_t read_to) {
    if (read_to < start_from + RECORD_SIZE) {
        return make_ready_future();
    }
    return f.dma_read_exactly<char>(start_from, RECORD_SIZE).then([=](temporary_buffer<char> buf) {
        // zrobiłem to kopiowanie, bo zarówno buf.prefix jak i buf.trim
        // jedyne co robią to _size = len, nie zmieniają bufora.
        std::string trimmed(buf.get(), RECORD_SIZE);
        assert(trimmed.size() == RECORD_SIZE);
        assert(trimmed.length() == RECORD_SIZE);
        save_to->emplace_back(move(trimmed));
        return read_records(f, save_to, start_from + RECORD_SIZE, read_to);
    });
}


/**
 * maximal number of record in one sorted chunk
 * written to disk as temporary file.
 */
inline uint64_t NUM_RECORDS_TO_READ_PER_THREAD() {
    uint64_t res = RAM_AVAILABLE() / RECORD_SIZE;
    res /= NUM_WORKERS();
    assert(res > 0 && "NUM_RECORDS_TO_READ_PER_THREAD: Not enough memory!");
    return res;
}


inline uint64_t NUM_BYTES_TO_READ_PER_THREAD() {
    return NUM_RECORDS_TO_READ_PER_THREAD() * RECORD_SIZE;
}


/**
 * pos - pozycja od której zaczynamy czytanie
 * po przeczytaniu znowu będziemy wołać read_chunk dla pos2 := pos + RAM_AVAILABLE,
 * zatem read_to w funkcji wyżej musi być pos + RAM_AVAILABLE (bo read() czyta do read_to - 1)
 */
future<> read_chunk(file f, uint64_t pos, uint64_t fsize, recordVector& out_vec) {
    uint64_t read_to = std::min(pos + NUM_BYTES_TO_READ_PER_THREAD(), fsize);
    return read_records(f, &out_vec, pos, read_to);
}


future<> dump_records_to_specific_pos(file f, recordVector& records, uint64_t starting_pos, uint64_t num_record = 0) {
    uint64_t write_pos = starting_pos + num_record * RECORD_SIZE;
    if (num_record > records.size() - 1)
        return make_ready_future();
    return f.dma_write<char>(write_pos, records[num_record].c_str(), RECORD_SIZE).then([=](size_t num) mutable {
        assert(num == RECORD_SIZE);
        return dump_records_to_specific_pos(f, records, starting_pos, num_record + 1);
    });
}

future<> dump_records(file f, recordVector& records) {
    return dump_records_to_specific_pos(f, records, 0, 0);
}

future<> dump_sorted_chunk(uint64_t num_chunk, recordVector& chunk) {
    sstring fname = tmp_chunk_name(num_chunk);
    return open_file_dma(fname, open_flags::rw | open_flags::create).then([chunk](file f) mutable {
        return dump_records(f, chunk);
    });
}

future<> worker_job(numVector& chunks_nums, std::string fname, uint64_t fsize) {
    for (uint64_t chunk_num : chunks_nums) {
        recordVector to_be_dumped;
        file f = open_file_dma(fname, open_flags::ro).get0();
        read_chunk(f, reading_pos(chunk_num), fsize, to_be_dumped).get();
        f.close();
        if (to_be_dumped.empty()) {
            return make_ready_future();
        }
        sort(to_be_dumped.begin(), to_be_dumped.end());
        // TODO wywalalo sie na shardzie 4, na pustym wektorze
        do_with(std::move(to_be_dumped), [=](recordVector& out_vec) mutable {
            uint64_t chunk_idx = chunk_num / NUM_RECORDS_TO_READ_PER_THREAD();
            return dump_sorted_chunk(chunk_idx, to_be_dumped);
        }).get();
    }
    return make_ready_future();
}


/**
 * delegates to each worker task of reading it's chunks, sorting them and saving
 * to disk
 */
future<> read_and_sort(std::vector<numVector> &workers_chunks_nums, uint64_t fsize) {
    static uint64_t i = 1; // 0 is for main shard, thus we start from thread 1
    return parallel_for_each(workers_chunks_nums.begin(), workers_chunks_nums.end(), [fsize](numVector &chunks_nums) {
        return smp::submit_to(i++, [&chunks_nums, fsize] {
            return async([&chunks_nums, fsize] {
                worker_job(chunks_nums, FNAME, fsize);
            });
        });
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
    numVector read_positions(std::ceil((double)NUM_RECORDS / (double)NUM_RECORDS_TO_READ_PER_THREAD()));
    static uint64_t i = 0;
    std::generate(read_positions.begin(), read_positions.end(), [] {
        auto res = i;
        i += NUM_RECORDS_TO_READ_PER_THREAD();
        return res;
    });
    i = 0;
    std::vector<numVector> res(NUM_WORKERS());
    uint64_t num_chunk = 0;
    for (uint64_t pos: read_positions) {
        res[i % NUM_WORKERS()].emplace_back(pos);
        i++;
    }
    std::cout << res;
    return std::make_pair(i, res);
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
                return f.close().then([&chunks_nums, fsize]{
                    return read_and_sort(chunks_nums, fsize);
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


