#if defined(SUPPORT_BEDROCK)

#include <Python.h>
#include <memory>
#include <map>
#include <iostream>
#include <iomanip>
#include <sstream>
#include <fstream>

#include "leveldb/db.h"
#include "leveldb/cache.h"
#include "leveldb/env.h"
#include "leveldb/decompress_allocator.h"
#include "leveldb/zlib_compressor.h"

class NullLogger : public leveldb::Logger {
public:
  void Logv(const char*, va_list) {
  }
};

static const int BlocksPerChunk = 16*16*16;

typedef std::map<std::string, leveldb::DB*> leveldb_ptrmap;
leveldb_ptrmap leveldb_reference;

namespace ChunkType {
enum {
  Data2D = 45,
  Data2DLegacy = 46,
  SubChunkPrefix = 47,
  LegacyTerrain = 48,
  BlockEntity = 49,
  Entity = 50,
  PendingTicks = 51,
  BlockExtraData = 52,
  BiomeState = 53,
  FinalizedState = 54,
  Version = 118
};
}

struct Data2DKey {
  static const size_t KeySize = 9;

  int32_t x;
  int32_t z;
  const uint8_t type = ChunkType::Data2D;

  operator const char*() const { return reinterpret_cast<const char*>(this); }  
};

struct Data2D {
  static const size_t ElementCount = 256;

  int16_t heightMap[ElementCount];
  int8_t biomes[ElementCount];
};

struct SubChunkKey {
  static const size_t KeySize = 10;
  static const size_t MaxSubChunkCount = 16;

  int32_t x;
  int32_t z;
  const uint8_t type = ChunkType::SubChunkPrefix;
  uint8_t subChunkIndex;

  operator const char*() const { return reinterpret_cast<const char*>(this); }  
};

struct SubChunkHeader {
  uint8_t version;
};

struct SubChunkV8Header {
  uint8_t storageGroupCount;
  uint8_t bitsPerTwoBlockIds;
};

struct SubChunkNBTHeader {
  uint8_t blockInfoLength;
  uint8_t unknown[3];
};

struct BlockData_6 {
  static const size_t StructsPerChunk = BlocksPerChunk / 32;

  uint8_t block1_id: 3;
  uint8_t block2_id: 3;
  uint8_t block3_id: 3;
  uint8_t block4_id: 3;
  uint8_t block5_id: 3;
  uint8_t block6_id: 3;
  uint8_t block7_id: 3;
  uint8_t block8_id: 3;
  uint8_t block9_id: 3;
  uint8_t block10_id: 3;
  uint8_t block11_id: 3;
  uint8_t block12_id: 3;
  uint8_t block13_id: 3;
  uint8_t block14_id: 3;
  uint8_t block15_id: 3;
  uint8_t block16_id: 3;
  uint8_t block17_id: 3;
  uint8_t block18_id: 3;
  uint8_t block19_id: 3;
  uint8_t block20_id: 3;
  uint8_t block21_id: 3;
  uint8_t block22_id: 3;
  uint8_t block23_id: 3;
  uint8_t block24_id: 3;
  uint8_t block25_id: 3;
  uint8_t block26_id: 3;
  uint8_t block27_id: 3;
  uint8_t block28_id: 3;
  uint8_t block29_id: 3;
  uint8_t block30_id: 3;
  uint8_t block31_id: 3;
  uint8_t block32_id: 3;
};

struct BlockData_8 {
  static const size_t StructsPerChunk = BlocksPerChunk / 2;

  uint8_t block1_id: 4;
  uint8_t block2_id: 4;
};

struct BlockData_10 {
  static const size_t StructsPerChunk = BlocksPerChunk / 32;

  uint8_t block1_id: 5;
  uint8_t block2_id: 5;
  uint8_t block3_id: 5;
  uint8_t block4_id: 5;
  uint8_t block5_id: 5;
  uint8_t block6_id: 5;
  uint8_t block7_id: 5;
  uint8_t block8_id: 5;
  uint8_t block9_id: 5;
  uint8_t block10_id: 5;
  uint8_t block11_id: 5;
  uint8_t block12_id: 5;
  uint8_t block13_id: 5;
  uint8_t block14_id: 5;
  uint8_t block15_id: 5;
  uint8_t block16_id: 5;
  uint8_t block17_id: 5;
  uint8_t block18_id: 5;
  uint8_t block19_id: 5;
  uint8_t block20_id: 5;
  uint8_t block21_id: 5;
  uint8_t block22_id: 5;
  uint8_t block23_id: 5;
  uint8_t block24_id: 5;
  uint8_t block25_id: 5;
  uint8_t block26_id: 5;
  uint8_t block27_id: 5;
  uint8_t block28_id: 5;
  uint8_t block29_id: 5;
  uint8_t block30_id: 5;
  uint8_t block31_id: 5;
  uint8_t block32_id: 5;
};

struct BlockData_12 {
  static const size_t StructsPerChunk = BlocksPerChunk / 16;

  uint8_t block1_id: 6;
  uint8_t block2_id: 6;
  uint8_t block3_id: 6;
  uint8_t block4_id: 6;
  uint8_t block5_id: 6;
  uint8_t block6_id: 6;
  uint8_t block7_id: 6;
  uint8_t block8_id: 6;
  uint8_t block9_id: 6;
  uint8_t block10_id: 6;
  uint8_t block11_id: 6;
  uint8_t block12_id: 6;
  uint8_t block13_id: 6;
  uint8_t block14_id: 6;
  uint8_t block15_id: 6;
  uint8_t block16_id: 6;
};

std::string convert_binary_to_hex(const std::string& binary) {
  std::ostringstream hex;
  hex << std::hex << std::setfill('0');
  for (size_t i = 0; i < binary.size(); i++) {
    hex << std::setw(2) << (static_cast<uint>(binary[i]) & 0xff);
  } 
  return hex.str();
}

void dump_to_file(const leveldb::Slice& slice, const std::string& value) {
  std::ostringstream filename;
  filename << "raw-" << convert_binary_to_hex(slice.ToString()) << ".dat";
  std::cout << "writing contents to " << filename.str() << "\n";

  std::ofstream dump(filename.str());
  dump.write(value.c_str(), value.size());
  dump.close();
}

bool processData2D(leveldb::DB* db, leveldb::ReadOptions& levelDbReadOptions, int x, int z, int version, PyObject* dict)
{
  Data2DKey key;
  key.x = x;
  key.z = z;

  std::string value;
  leveldb::Status status = db->Get(levelDbReadOptions, leveldb::Slice(key, key.KeySize), &value);
  if (status.code()) {
    char errorBuff[1024];
    sprintf(errorBuff, "Error processing Data2D@chunk (%d, %d): %d", x, z, status.code());
    PyErr_SetString(PyExc_RuntimeError, errorBuff);
    return false;
  }

  const Data2D* data = reinterpret_cast<const Data2D*>(value.c_str());

  PyObject* heightmap = PyTuple_New(Data2D::ElementCount);
  for (size_t i=0; i < Data2D::ElementCount; i++) {
    PyTuple_SetItem(heightmap, i, PyInt_FromLong((long)data->heightMap[i]));
  }

  PyObject* biomes = PyString_FromStringAndSize(reinterpret_cast<const char*>(data->biomes), Data2D::ElementCount);

  PyDict_SetItemString(dict, "Biomes", biomes);
  PyDict_SetItemString(dict, "HeightMap", heightmap);

  return true;
}

bool processSubChunks(leveldb::DB* db, leveldb::ReadOptions& levelDbReadOptions, int x, int z, PyObject* dict)
{
  PyObject* list = PyList_New(0);
  for (size_t subChunkIndex = 0; subChunkIndex < SubChunkKey::MaxSubChunkCount; subChunkIndex++) {
    SubChunkKey key;
    key.x = x;
    key.z = z;
    key.subChunkIndex = subChunkIndex;

    std::string value;
    leveldb::Slice slice(key, key.KeySize); 
    leveldb::Status status = db->Get(levelDbReadOptions, slice, &value);
    if (status.code() == leveldb::Status::kOk) {
      const char* raw = value.c_str();
      size_t size = value.size();

      const SubChunkHeader* header = reinterpret_cast<const SubChunkHeader*>(raw);
      raw += sizeof(SubChunkHeader);
      size -= sizeof(SubChunkHeader);

      switch (header->version) {
        case 0x8:
        {
          const SubChunkV8Header* header_v8 = reinterpret_cast<const SubChunkV8Header*>(raw);
          raw += sizeof(SubChunkV8Header);
          size -= sizeof(SubChunkV8Header);
          std::cout << "SubChunk index:" << std::dec << subChunkIndex << std::endl;
          std::cout << "\tversion:" << (uint32_t) header->version << std::endl;
          std::cout << "\tbytes: " << value.size() << std::endl;
          std::cout << "\tstorageGroupCount:" << (uint32_t) header_v8->storageGroupCount << std::endl;
          std::cout << "\tbitsPerBlock:" << (uint32_t) header_v8->bitsPerTwoBlockIds << std::endl;
          std::cout << "\tstartingAddress:" << std::hex << (void*)value.c_str();
          //std::cout << "\tblockinfocount:" << dataV8->storageGroupData.blockInfoCount << std::endl;
          //std::cout << "\tunknown:" << (uint16_t) dataV8->storageGroupData.unknown[0] << "," << (uint16_t) dataV8->storageGroupData.unknown[1] << "," << (uint16_t) dataV8->storageGroupData.unknown[2] << std::endl;
          dump_to_file(slice, value);
 
          std::ostringstream block_id_stream;
          std::string raw_block_bytes;
          switch (header_v8->bitsPerTwoBlockIds) {
           case 0x6:
            {    
              const BlockData_6* block = reinterpret_cast<const BlockData_6*>(raw);

              for (size_t i = 0; i < BlockData_6::StructsPerChunk; i++) {
                block_id_stream << block->block1_id << block->block2_id << block->block3_id << block->block4_id;
                block_id_stream << block->block5_id << block->block6_id << block->block7_id << block->block8_id;
                block_id_stream << block->block9_id << block->block10_id << block->block11_id << block->block12_id;
                block_id_stream << block->block13_id << block->block14_id << block->block15_id << block->block16_id;
                block_id_stream << block->block17_id << block->block18_id << block->block19_id << block->block20_id;
                block_id_stream << block->block21_id << block->block22_id << block->block23_id << block->block24_id;
                block_id_stream << block->block25_id << block->block26_id << block->block27_id << block->block28_id;
                block_id_stream << block->block29_id << block->block30_id << block->block31_id << block->block32_id;
                block++;
              }

              std::cout << "blockdata: " << std::hex << (void*) raw << "," << (void*) block << "," << std::dec << ((const char*) block - (const char*) raw) << "," << sizeof(BlockData_6) << std::endl;
              std::string blockdata(raw, 1640);   
              raw += 1640;
              std::cout << std::dec << size << std::endl;
              size -= 1640;
              std::cout << size << std::endl;

              raw_block_bytes = blockdata;

              std::cout << convert_binary_to_hex(blockdata) << std::endl << std::endl;
            } break;
            case 0x8:
            {    
              const BlockData_8* block = reinterpret_cast<const BlockData_8*>(raw);
              for (size_t i = 0; i < BlockData_8::StructsPerChunk; i++) {
                block_id_stream << block->block1_id << block->block2_id;
                block++;
              }

              std::cout << "blockdata: " << std::hex << (void*) raw << "," << (void*) block << "," << std::dec << ((const char*) block - (const char*) raw) << "," << sizeof(BlockData_8) << std::endl;
              std::string blockdata(raw, 2048);   
              raw += 2048;
              std::cout << std::dec << size << std::endl;
              size -= 2048;
              std::cout << size << std::endl;

              std::cout << convert_binary_to_hex(blockdata) << std::endl << std::endl;

              raw_block_bytes = blockdata;

            } break;
            case 0xa:
            {    
              const BlockData_10* block = reinterpret_cast<const BlockData_10*>(raw);
              for (size_t i = 0; i < BlockData_10::StructsPerChunk; i++) {
                block_id_stream << block->block1_id << block->block2_id << block->block3_id << block->block4_id;
                block_id_stream << block->block5_id << block->block6_id << block->block7_id << block->block8_id;
                block_id_stream << block->block9_id << block->block10_id << block->block11_id << block->block12_id;
                block_id_stream << block->block13_id << block->block14_id << block->block15_id << block->block16_id;
                block_id_stream << block->block17_id << block->block18_id << block->block19_id << block->block20_id;
                block_id_stream << block->block21_id << block->block22_id << block->block23_id << block->block24_id;
                block_id_stream << block->block25_id << block->block26_id << block->block27_id << block->block28_id;
                block_id_stream << block->block29_id << block->block30_id << block->block31_id << block->block32_id;
                block++;
              }

              std::cout << "blockdata: " << std::hex << (void*) raw << "," << (void*) block << "," << std::dec << ((const char*) block - (const char*) raw) << "," << sizeof(BlockData_10) << std::endl;
              std::string blockdata(raw, 2732);   
              raw += 2732;
              std::cout << std::dec << size << std::endl;
              size -= 2732;
              std::cout << size << std::endl;

              std::cout << convert_binary_to_hex(blockdata) << std::endl << std::endl;

              raw_block_bytes = blockdata;

            } break;
            case 0xc:
            {    
              const BlockData_12* block = reinterpret_cast<const BlockData_12*>(raw);
              for (size_t i = 0; i < BlockData_12::StructsPerChunk; i++) {
                block_id_stream << block->block1_id << block->block2_id << block->block3_id << block->block4_id;
                block_id_stream << block->block5_id << block->block6_id << block->block7_id << block->block8_id;
                block_id_stream << block->block9_id << block->block10_id << block->block11_id << block->block12_id;
                block_id_stream << block->block13_id << block->block14_id << block->block15_id << block->block16_id;
                block++;
              }

              std::cout << "blockdata: " << std::hex << (void*) raw << "," << (void*) block << "," << std::dec << ((const char*) block - (const char*) raw) << "," << sizeof(BlockData_12) << std::endl;
              std::string blockdata(raw, 3280);   
              raw += 3280;
              std::cout << std::dec << size << std::endl;
              size -= 3280;
              std::cout << size << std::endl;

              std::cout << convert_binary_to_hex(blockdata) << std::endl << std::endl;

              raw_block_bytes = blockdata;

            } break;
            default:
            {
              std::cout << "skipping unknown bitsPerTwoBlockIds: " << std::hex << (uint16_t) header_v8->bitsPerTwoBlockIds << "\n";
            } break;
          }

          std::string block_id = block_id_stream.str();

          const SubChunkNBTHeader* nbt_header = reinterpret_cast<const SubChunkNBTHeader*>(raw);
          raw += sizeof(SubChunkNBTHeader);
          size -= sizeof(SubChunkNBTHeader);

          std::cout << "nbt: " << std::hex << (void*) raw << ", blockInfoLength:" << std::dec <<  (uint16_t) nbt_header->blockInfoLength << ", unknown:" << (uint16_t) nbt_header->unknown[0] << "," << (uint16_t) nbt_header->unknown[1] << "," << (uint16_t) nbt_header->unknown[2] << std::endl;
          std::string nbt(raw, size);
          std::cout << convert_binary_to_hex(nbt) << std::endl << std::endl;

          PyObject* sectionDict = PyDict_New();
          PyDict_SetItemString(sectionDict, "_blockinfo", PyString_FromStringAndSize(nbt.c_str(), nbt.size()));
          PyDict_SetItemString(sectionDict, "_blockinfo_length", PyInt_FromLong(nbt_header->blockInfoLength));
          PyDict_SetItemString(sectionDict, "_blockdata", PyString_FromStringAndSize(block_id.c_str(), block_id.size()));
          PyDict_SetItemString(sectionDict, "_raw_blockdata", PyString_FromStringAndSize(raw_block_bytes.c_str(), raw_block_bytes.size()));
          PyDict_SetItemString(sectionDict, "_bitsPerBlock", PyInt_FromLong(header_v8->bitsPerTwoBlockIds >> 1));
          PyDict_SetItemString(sectionDict, "Y", PyInt_FromLong(subChunkIndex));
          PyList_Append(list, sectionDict);
          break;
        }
        default:
          char errorBuff[1024];
          sprintf(errorBuff, "Unexpected chunk version@chunk (%d, %d): %d", x, z, header->version);
          PyErr_SetString(PyExc_RuntimeError, errorBuff);
          return false;      
      }
      

    }
  }

  PyDict_SetItemString(dict, "Sections", list);
  return true;
}

extern "C" {

PyObject* leveldb_open(PyObject *self, PyObject *args) {
  char *leveldb_path = NULL;
  if (!PyArg_ParseTuple(args, "s", &leveldb_path)) {
    return NULL;
  }

  //todo: guard against reopening an open db

  std::unique_ptr<leveldb::Options> options(new leveldb::Options);
  options->create_if_missing = false;
  options->block_size = BlocksPerChunk;

  // start: suggestions from leveldb/mcpe_sample_setup.cpp
  options->block_cache = leveldb::NewLRUCache(40 * 1024 * 1024);
  options->write_buffer_size = 4 * 1024 * 1024;
  options->info_log = new NullLogger();
  options->compressors[0] = new leveldb::ZlibCompressorRaw(-1);
  // also setup the old, slower compressor for backwards compatibility. This will only be used to read old compressed blocks.
  options->compressors[1] = new leveldb::ZlibCompressor();
  // end: suggestions from leveldb/mcpe_sample_setup.cpp

  leveldb::DB* db;
  leveldb::Status status = leveldb::DB::Open(*options, leveldb_path, &db);

  if (status.code()) {
    char errorBuff[1024];
    sprintf(errorBuff, "Error opening levelDB: %d", status.code());
    PyErr_SetString(PyExc_RuntimeError, errorBuff);
    return NULL;
  }

  leveldb_reference.insert(leveldb_ptrmap::value_type(leveldb_path, db));
  return PyString_FromString(leveldb_path);
}

PyObject* leveldb_close(PyObject *self, PyObject *args) {
  char *leveldb_path = NULL;
  if (!PyArg_ParseTuple(args, "s", &leveldb_path)) {
    return NULL;
  }

  leveldb_ptrmap::iterator i = leveldb_reference.find(leveldb_path);
  if (i != leveldb_reference.end()) {
    leveldb::DB* db = (*i).second;
    delete db;
    leveldb_reference.erase(i);
  }

  return NULL;
}

PyObject* leveldb_get_chunk_keys(PyObject *self, PyObject *args) {
  char *leveldb_path = NULL;
  if (!PyArg_ParseTuple(args, "s", &leveldb_path)) {
    return NULL;
  }

  leveldb_ptrmap::iterator i = leveldb_reference.find(leveldb_path);
  if (i == leveldb_reference.end()) {
    char errorBuff[1024];
    sprintf(errorBuff, "Attempted to reference unopened leveldb: %s", leveldb_path);
    PyErr_SetString(PyExc_RuntimeError, errorBuff);
    return NULL;
  }

  leveldb::DB* db = (*i).second;
  leveldb::ReadOptions levelDbReadOptions;
  leveldb::DecompressAllocator allocator;
  levelDbReadOptions.fill_cache = false;
  levelDbReadOptions.decompress_allocator = &allocator;
  
  leveldb::Iterator* iter = db->NewIterator(levelDbReadOptions);
  PyObject* retval = PyDict_New();
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {

    leveldb::Slice slice = iter->key();
    size_t keysize = slice.size();
    const char* key = slice.data();

    if (keysize == Data2DKey::KeySize) {
      const Data2DKey *chunk = reinterpret_cast<const Data2DKey*>(key);
      if (chunk->type == ChunkType::Data2D) {
        PyObject* value = PyDict_New();
        PyDict_SetItemString(value, "_keysize", PyInt_FromSize_t(keysize));
        PyDict_SetItemString(value, "_version", PyInt_FromLong(2));
        PyDict_SetItemString(value, "x", PyInt_FromLong(chunk->x));
        PyDict_SetItemString(value, "z", PyInt_FromLong(chunk->z));
        
        PyDict_SetItem(retval, PyString_FromFormat("%d,%d", chunk->x, chunk->z), value);
      }
    }
  }

  return retval;
}

PyObject* leveldb_get_chunk_data(PyObject *self, PyObject *args) {
  char *leveldb_path = NULL;
  int x,z,version;
  if (!PyArg_ParseTuple(args, "siii", &leveldb_path, &x, &z, &version)) {
    return NULL;
  }

  leveldb_ptrmap::iterator i = leveldb_reference.find(leveldb_path);
  if (i == leveldb_reference.end()) {
    char errorBuff[512];
    sprintf(errorBuff, "Attempted to reference unopened leveldb: %s", leveldb_path);
    PyErr_SetString(PyExc_RuntimeError, errorBuff);
    return NULL;
  }

  leveldb::DB* db = (*i).second;
  leveldb::ReadOptions levelDbReadOptions;
  leveldb::DecompressAllocator allocator;
  levelDbReadOptions.fill_cache = false;
  levelDbReadOptions.decompress_allocator = &allocator;

  PyObject* retval = PyDict_New();

  PyDict_SetItemString(retval, "xPos", PyInt_FromLong(x));
  PyDict_SetItemString(retval, "xPos", PyInt_FromLong(z));
  
  if (!processData2D(db, levelDbReadOptions, x, z, version, retval)) {
    return NULL;
  }

  if (!processSubChunks(db, levelDbReadOptions, x, z, retval)) {
    return NULL;
  }

  return retval;
}

} //extern "C"
#endif //#if defined(SUPPORT_BEDROCK)