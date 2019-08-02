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
using stringVector = std::vector<std::string>;
using numVector = std::vector<uint64_t>;

static const uint64_t RECORD_SIZE = 4096;

static std::vector<recordVector> CHUNKS; // used in merging phase, CHUNKS[i] contains

static numVector TO_BE_TAKEN; // number of record from CHUNKS[i] to be taken in next disc load

static stringVector OUT_FNAMES; // output (to-be-merged) filenames

static std::vector<recordVector> OUT_MERGE_BUFFER; // here are put records from to-be-merged output files



inline uint64_t RAM_AVAILABLE() {
//    return memory::stats().total_memory();
    return RECORD_SIZE * 18;
}

inline uint64_t NUM_WORKERS() {
    return smp::count - 1;
}


inline sstring out_chunk_name(uint64_t const num_chunk) {
    return sstring(OUT_DIR + std::string("chunk") + std::to_string(num_chunk));
}

inline sstring tmp_chunk_name(uint64_t const num_chunk) {
    return sstring(TMP_DIR + std::string("chunk") + std::to_string(num_chunk));
}

inline uint64_t reading_pos(uint64_t const num_chunk) {
    return num_chunk * RECORD_SIZE;
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


future<> read_records(file f, recordVector &save_to, uint64_t start_from, uint64_t const read_to) {
    assert(start_from % RECORD_SIZE == 0);
    assert(read_to % RECORD_SIZE == 0);
    if (read_to < start_from + RECORD_SIZE) {
        return make_ready_future();
    }
    temporary_buffer<char> buf = f.dma_read_exactly<char>(start_from, RECORD_SIZE).get0();
    std::string trimmed(buf.get(), RECORD_SIZE);
    save_to.emplace_back(std::move(trimmed));
    return read_records(f, save_to, start_from + RECORD_SIZE, read_to);
}

/**
 * pos - pozycja od której zaczynamy czytanie
 * po przeczytaniu znowu będziemy wołać read_chunk dla pos2 := pos + NUM_BYTES_TO_READ_PER_THREAD(),
 * zatem read_to w funkcji wyżej musi być pos + NUM_BYTES_TO_READ_PER_THREAD() (bo read() czyta do read_to - 1)
 */
future<> read_chunk(file f, uint64_t pos, uint64_t fsize, recordVector &out_vec) {
    uint64_t read_to = std::min(pos + NUM_BYTES_TO_READ_PER_THREAD(), fsize);
    assert(read_to % RECORD_SIZE == 0);
    assert(pos % RECORD_SIZE == 0);
    return read_records(f, out_vec, pos, read_to);
}


future<> dump_records_to_specific_pos(file f, recordVector &records, uint64_t starting_pos, uint64_t num_record = 0) {
    uint64_t write_pos = starting_pos + num_record * RECORD_SIZE;
    if (num_record > records.size() - 1)
        return make_ready_future();
    size_t num = f.dma_write<char>(write_pos, records[num_record].c_str(), RECORD_SIZE).get0();
    assert(num == RECORD_SIZE);
    return dump_records_to_specific_pos(f, records, starting_pos, num_record + 1);
}

future<> dump_records(file f, recordVector &records) {
    return async([&records, f] {
        return dump_records_to_specific_pos(f, records, 0, 0).get0();
    });
}

future<> dump_sorted_chunk(uint64_t num_chunk, recordVector &chunk) {
    sstring fname = tmp_chunk_name(num_chunk);
    return open_file_dma(fname, open_flags::wo | open_flags::create).then([&chunk](file f) mutable {
        return dump_records(f, chunk);
    });
}

future<> worker_job(numVector &chunks_nums, std::string fname, uint64_t fsize) {
    for (uint64_t chunk_num : chunks_nums) {
        recordVector to_be_dumped;

        do_with(std::move(to_be_dumped), [chunk_num, fname, fsize](recordVector &out_vec) mutable {
            file f = open_file_dma(fname, open_flags::ro).get0();
            read_chunk(f, reading_pos(chunk_num), fsize, out_vec).get();
            f.close(); // .get();
            sort(out_vec.begin(), out_vec.end());
            uint64_t chunk_idx = chunk_num / NUM_RECORDS_TO_READ_PER_THREAD();
            return do_with(std::move(out_vec), [chunk_idx](recordVector &out) {
                return dump_sorted_chunk(chunk_idx, out);
            });
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
                return worker_job(chunks_nums, FNAME, fsize).get0();
            });
        });
    });
}

/**
 * returns vector of size NUM_WORKERS(), where each worker is given
 * vector of indices (counting from 0) of records, from which they should start
 * reading chunks. for instance, given [3, 8] vector, thread reads two chunks, first
 * starting at 3 * RECORD_SIZE and second at 8 * RECORD_SIZE position, both containing
 * maximally NUM_RECORDS_TO_READ_PER_THREAD() of records
 */
std::pair<uint64_t, std::vector<numVector>> chunks_to_be_read(uint64_t fsize) {
    uint64_t NUM_RECORDS = fsize / RECORD_SIZE;
    numVector read_positions(std::ceil((double) NUM_RECORDS / (double) NUM_RECORDS_TO_READ_PER_THREAD()));
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

/////////////////////////////////////////////////////////////////////////////////////
///      MERGING PHASE HERE      ////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////



inline uint64_t MERGING_BLOCK_NUM_RECORDS() {
    return RAM_AVAILABLE() / RECORD_SIZE / 10;
}

inline uint64_t MAX_RECORDS_PER_WORKER() {
    return RAM_AVAILABLE() / RECORD_SIZE / NUM_WORKERS();
}

future<> _read_records(file f, recordVector *save_to, uint64_t start_from, uint64_t read_to) {
    if (read_to < start_from + RECORD_SIZE) {
        return make_ready_future();
    }
    return f.dma_read_exactly<char>(start_from, RECORD_SIZE).then([=](temporary_buffer<char> buf) {
        // zrobiłem to kopiowanie, bo zarówno buf.prefix jak i buf.trim
        // jedyne co robią to _size = len, nie zmieniają bufora.
        std::string trimmed(buf.get(), RECORD_SIZE);
        assert(trimmed.size() == RECORD_SIZE);
        assert(trimmed.length() == RECORD_SIZE);
        save_to->emplace_back(std::move(trimmed));
        return _read_records(f, save_to, start_from + RECORD_SIZE, read_to);
    });
}


// może wczytać 0 lub więcej rekordów, maksymalnie `num_rec`
future<> _read_specific_record_num(file f, uint64_t fsize, recordVector *save_to,
                                  uint64_t num_rec, uint64_t start_reading_from_rec = 0) {

    uint64_t start_pos = start_reading_from_rec * RECORD_SIZE;
    uint64_t read_to = std::min(fsize, start_pos + num_rec * RECORD_SIZE);
    return _read_records(f, save_to, start_pos, read_to);
}

/**
 * @param chunk number of chunk we read from
 * @param num_records number of records to be read
 * @param starting_record record we start reading from
 */
future<> read_chunk_file(uint64_t chunk, uint64_t num_records) {
    sstring fname = tmp_chunk_name(chunk);
    return open_file_dma(fname, open_flags::rw).then([=](file f) mutable {
        return f.size().then([=](uint64_t fsize) mutable {
            assert(CHUNKS[chunk].empty());
            return _read_specific_record_num(f, fsize, &CHUNKS[chunk], num_records, TO_BE_TAKEN[chunk]).then([=](){
                reverse(CHUNKS[chunk].begin(), CHUNKS[chunk].end());
                TO_BE_TAKEN[chunk] += CHUNKS[chunk].size();
                return make_ready_future<>();
            });
        });
    });
}


future<> _dump_records_to_specific_pos(file f, recordVector& records, uint64_t starting_pos, uint64_t num_record = 0) {
    uint64_t write_pos = starting_pos + num_record * RECORD_SIZE;
    if (num_record > records.size() - 1)
        return make_ready_future();
    return f.dma_write<char>(write_pos, records[num_record].c_str(), RECORD_SIZE).then([=](size_t num) mutable {
        assert(num == RECORD_SIZE);
        return _dump_records_to_specific_pos(f, records, starting_pos, num_record + 1);
    });
}

future<> _dump_records(file f, recordVector& records) {
    return _dump_records_to_specific_pos(f, records, 0, 0);
}

future<stop_iteration> dump_output(recordVector& out_vec) {
    static uint64_t chunk_num = 0;
    sstring fname = out_chunk_name(chunk_num);
    OUT_FNAMES.push_back(fname);
    return open_file_dma(fname, open_flags::rw | open_flags::create).then([&out_vec](file f) {
        return _dump_records(f, out_vec).then([&out_vec](){
            out_vec.clear();
            chunk_num++;
            return make_ready_future<stop_iteration>(stop_iteration::no);
        });
    });
}


future<stop_iteration> extract_min(uint64_t records_per_chunk, recordVector &out_vec) {
    int64_t min_idx = -1;
    bool sth_left = false;
    for (uint64_t i = 0; i < CHUNKS.size(); i++) {
        if (!CHUNKS[i].empty()) {
            sth_left = true;
            if (min_idx < 0) {
                min_idx = i;
            } else if (CHUNKS[i].back() < CHUNKS[min_idx].back()) {
                min_idx = i;
            }
        }
    }
    if (!sth_left) {
        if (!out_vec.empty()) {
            return dump_output(out_vec);
        }
        return make_ready_future<stop_iteration>(stop_iteration::yes);
    }

    out_vec.push_back(CHUNKS[min_idx].back());
    CHUNKS[min_idx].pop_back();
    if (CHUNKS[min_idx].empty()) {
        // need to load more data from disk
        return read_chunk_file(min_idx, records_per_chunk).then([&out_vec]() {
            if (out_vec.size() == MERGING_BLOCK_NUM_RECORDS()) {
                return dump_output(out_vec);
            }
            return make_ready_future<stop_iteration>(stop_iteration::no);
        });
    }
    if (out_vec.size() == MERGING_BLOCK_NUM_RECORDS()) {
        return dump_output(out_vec);
    }
    return make_ready_future<stop_iteration>(stop_iteration::no);
}

future<> init_merge_phase(uint64_t chunks, uint64_t records_per_chunk) {
    numVector tmp(chunks);
    uint64_t i = 0;
    for (uint64_t &el : tmp)
        el = i++;
    if (chunks > 1)
        assert(tmp[1] == 1);
    return parallel_for_each(tmp.begin(), tmp.end(), [=](uint64_t num_chunk) {
        return read_chunk_file(num_chunk, records_per_chunk);
    });
}

/**
 * Plik wejśćiowy mamy podzielony na bloki o rozmiarze maksymalnie RAM_AVAILABLE (./tmp/chunkN),
 * w fazie mergowania bierzemy po kawałku każdego z bloków, następnie szukamy elementów
 * globalnie najmniejszych, które dołączamy do wynikowego bloku, który następnie zapiszemy na dysk
 * (wynikowy plik jest konkatenacją tychże bloków)
 * @param chunks ilośc stworzonych plików chunksN
 * @return wektor nazw plików, które należy skonkatenować, by otrzymać posortowany plik wejściowy.
 */
future<stringVector> merge_phase(uint64_t chunks) {
    uint64_t RAM_FOR_CHUNKS = RAM_AVAILABLE() - MERGING_BLOCK_NUM_RECORDS() * RECORD_SIZE;
    uint64_t RECS_PER_CHUNK = RAM_FOR_CHUNKS / RECORD_SIZE / chunks;
    assert(RECS_PER_CHUNK > 0);
    recordVector out_batch = recordVector();
    CHUNKS = std::vector<recordVector>(chunks);
    TO_BE_TAKEN = std::vector<uint64_t>(chunks);
    OUT_FNAMES = std::vector<std::string>();
    return
            do_with(std::move(out_batch),
                    [=](
                            recordVector &out_vec
                    ) mutable {
                        return
                                init_merge_phase(chunks, RECS_PER_CHUNK
                                ).then([=]() mutable {
                                    return repeat([=]() mutable {
                                        return
                                                extract_min(RECS_PER_CHUNK, out_vec
                                                );
                                    }).then([]() {
                                        return make_ready_future<std::vector <std::string>>
                                        (OUT_FNAMES);
                                    });
                                });
                    });

}


void prepareMergePhase(uint64_t num_workers) {
    assert(MAX_RECORDS_PER_WORKER() > 0 && "Cannot perform multithread merging! Not enough RAM amount");
    for (auto el : OUT_MERGE_BUFFER) {
        el.clear();
    }
    OUT_MERGE_BUFFER.clear();
    for (uint64_t i = 0; i < num_workers; i++) {
        OUT_MERGE_BUFFER.emplace_back(recordVector());
    }
    TO_BE_TAKEN.clear();
    for (uint64_t i = 0; i < num_workers; i++) {
        TO_BE_TAKEN.emplace_back(0);
    }
}


future<> read_and_dump_output_batch(file f1, uint64_t fsize1, file f2, uint64_t fsize2, uint64_t my_num_worker) {
    return _read_specific_record_num(f2, fsize2, &OUT_MERGE_BUFFER[my_num_worker],
                                    MAX_RECORDS_PER_WORKER(), TO_BE_TAKEN[my_num_worker]).then([=]() {
        uint64_t records_written = OUT_MERGE_BUFFER[my_num_worker].size();
        if (records_written == 0)
            return make_ready_future<>();
        TO_BE_TAKEN[my_num_worker] += OUT_MERGE_BUFFER[my_num_worker].size();
        return _dump_records_to_specific_pos(f1, OUT_MERGE_BUFFER[my_num_worker], fsize1).then([=]() {
            OUT_MERGE_BUFFER[my_num_worker].clear();
            return read_and_dump_output_batch(f1, fsize1 + records_written * RECORD_SIZE, f2, fsize2, my_num_worker);
        });
    });
}


// appends fname2 content to fname1
future<> merge2(std::string fname1, std::string fname2, uint64_t my_num_worker) {
    return open_file_dma(fname1, open_flags::rw).then([=](file f1) mutable {
        return f1.size().then([=](uint64_t fsize1) mutable {
            return open_file_dma(fname2, open_flags::rw).then([=](file f2) mutable {
                return f2.size().then([=](uint64_t fsize2) mutable {
                    assert(fsize1 % RECORD_SIZE == 0);
                    assert(fsize2 % RECORD_SIZE == 0);
                    return read_and_dump_output_batch(f1, fsize1, f2, fsize2, my_num_worker).then([=]() {
                        return remove_file(fname2);
                    });
                });
            });
        });
    });
}


// Mergowanie wynikowych plików odbywa się równolegle, w każdej fazie
// dla N plików pracuje N / 2 wątków. (liczba plików zmniejsza się z czasem)
future<> do_merging(stringVector fnames) {
    numVector first_indices;
    size_t out_files_num = fnames.size();
    assert (out_files_num > 1);

    for (uint64_t i = 0; i < out_files_num; i++) {
        if (i % 2 == 0 && i != out_files_num - 1)
            first_indices.push_back(i);
    }
    prepareMergePhase(first_indices.size());
    return parallel_for_each(first_indices.begin(), first_indices.end(), [=](uint64_t num_first) {
        return merge2(fnames[num_first], fnames[num_first + 1], num_first / 2);
    });
}


future<> merge_output_files(stringVector fnames) {
    if (fnames.size() == 1) {
        return make_ready_future<>();
    }
    return do_merging(fnames).then([=]() {
        stringVector trimmed_fnames;
        uint64_t i = 0;
        for (auto el : fnames) {
            if (i % 2 == 0) {
                trimmed_fnames.emplace_back(fnames[i]);
            }
            i++;
        }
        return merge_output_files(trimmed_fnames);
    });
}


future<> external_sort() {
    static sstring fname(FNAME);
    return open_file_dma(fname, open_flags::rw).then([](file f) {
        return f.size().then([f](uint64_t fsize) mutable {
            assert(fsize % RECORD_SIZE == 0 &&
                   "Malformed input file error! It's size must be divisible by record size");
            std::pair<uint64_t, std::vector<numVector>> res = chunks_to_be_read(fsize);
            uint64_t num_chunks = res.first;
            std::vector<numVector> chunks_nums = res.second;
            return do_with(std::move(chunks_nums), [=](std::vector<numVector> &chunks_nums_vec) mutable {
                return f.close().then([=, &chunks_nums_vec] {
                    return read_and_sort(chunks_nums_vec, fsize).then([=]() {
                        return merge_phase(num_chunks).then([](std::vector<std::string> fnames) {
                            return merge_output_files(fnames);
                        });
                    });
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


int main(int argc, char **argv) {
    uint64_t MERGING_BLOCK_NUM_RECORDS = RAM_AVAILABLE() / RECORD_SIZE / 10;

    compute(argc, argv);
    std::cout << "Posortowany plik znajduje się w katalogu " << OUT_DIR << " pod nazwą chunk0" << std::endl;
    return 0;
}


