"""Filesystem Cache

This library provides the functions and objects required to save filesystem views 
in a cache. The file systems are stored in a object store that follows the git
scm format. 

Since this libary is intended to work on filesystem that can have large files,
this library attempts to use streams when possible to avoid requiring to load
flies fully into memory.
"""

import io, os, hashlib, zlib
from collections import namedtuple
from contextlib import contextmanager

def obj_header(objtype, length):
    '''Constructs a git object store object header'''
    return (objtype + '\x20' + str(length) + '\x00').encode()

def write_compressed(streams, fout, block_size=4096):
    '''Compresses streams and writes to output'''
    zlib_obj = zlib.compressobj()
    for stream in streams:
        while True:
            chunk = stream.read(block_size)
            if not chunk:
                break
            print("RAW", len(chunk))
            compressed_bytes = zlib_obj.compress(chunk)
            if compressed_bytes:
                print("COMPRESSED", len(compressed_bytes))
                fout.write(compressed_bytes)
    
    # Flush out remaining compressed bytes
    compressed_bytes = zlib_obj.flush()
    if compressed_bytes:
        print("FLUSHED", len(compressed_bytes))
        fout.write(compressed_bytes)
            
def write_obj(store_path, sha, header, data):
    '''Writes a git object to a git object store'''
    dir_path = os.path.join(store_path, sha[:2])
    path = os.path.join(dir_path, sha[2:])
    os.makedirs(dir_path, exist_ok=True)

    with open(path, 'wb') as fout:
        write_compressed([header, data], fout)
        
def compute_sha1_hash(*streams, block_size=4096):
    '''Computes sha1 hash of the concatinated streams'''
    sha = hashlib.sha1()
    for stream in streams:
        while True:
            data = stream.read(block_size)
            if not data:
                break    
            sha.update(data)
    return sha.hexdigest()
        
class GitObject(object):
    def __init__(self, objtype, data_path=None, data_stream=None):
        # Can only have one of path or stream
        if data_path is not None:
            assert data_stream is None, "Cannot provide both stream and path"
        else:
            assert data_stream is not None, "Must provide stream or path"
            
        self.objtype = objtype
        self.path = data_path
        self.stream = data_stream
        
        self._hash = None
        self._len = None
    
    @property
    def hash(self):
        if self._hash is None:
            with self.header() as hstream, self.data() as dstream:
                self._hash = compute_sha1_hash(hstream, dstream)
        return self._hash
    
    def __len__(self):
        if self._len is None:
            with self.data() as dstream:
                dstream.seek(0, os.SEEK_END)
                self._len = dstream.tell()
        return self._len
    
    @contextmanager
    def header(self):
        header = obj_header(self.objtype, len(self))
        yield io.BytesIO(header)
    
    @contextmanager
    def data(self):
        f = None
        try:
            if self.path is not None:
                f = open(self.path, 'rb')
                yield f
            else:
                self.stream.seek(0)
                yield self.stream
        finally:
            if f is not None:
                f.close()
                
    def __repr__(self):
        with self.header() as hstream:
            return hstream.read().decode() + ' ' + self.hash
        
class BlobObject(GitObject):
    def __init__(self, data_path=None, data_stream=None):
        super(BlobObject, self).__init__('blob', data_path=data_path, data_stream=data_stream)


