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
using namespace std;


static const string FNAME = string("./input.txt"); // TODO tu należy wpisać nazwę pliku wejściowego
static const string TMP_DIR = string("./tmp/");
static const string OUT_DIR = string("./out/");

using recordVector = std::vector<string>;

static const uint64_t RECORD_SIZE = 4096;


inline uint64_t RAM_AVAILABLE() {
    return memory::stats().total_memory();
}

inline uint64_t NUM_WORKERS() {
    return memory::stats().total_memory() - 1;
}

future<> test() {
    cout << "TEST: " << seastar::smp::count;
    return make_ready_future();
}

//future<> external_sort() {
//    static sstring fname(FNAME);
//    static uint64_t i = 0;
//    return open_file_dma(fname, open_flags::rw).then([](file f) {
//        return f.size().then([f](uint64_t fsize) mutable {
//            return handle_chunks(f, fsize).then([](uint64_t chunks){
//                return merge_phase(chunks).then([](vector<string> fnames){
////                    return make_ready_future(); // FAK
//                    return merge_output_files(fnames);
//                });
//            });
//        });
//    });
//}


int compute(int argc, char **argv) {
    app_template app;
    try {
        app.run(argc, argv, test);
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
    cout << "Posortowany plik znajduje się w katalogu " << OUT_DIR << " pod nazwą chunk0" << endl;
    return 0;
}


