use crate::heap_page::HeapPage;
use crate::page::Page;
use common::prelude::*;
use common::PAGE_SIZE;
use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::{Arc, RwLock};

use std::io::BufWriter;
use std::io::{Seek, SeekFrom};

/// The struct for a heap file.  
///
/// HINT: You likely will want to design for interior mutability for concurrent accesses.
/// eg Arc<RwLock<>> on some internal members
///
/// HINT: You will probably not be able to serialize HeapFile, as it needs to maintain a link to a
/// File object, which cannot be serialized/deserialized/skipped by serde. You don't need to worry
/// about persisting read_count/write_count during serialization.
///
/// Your code should persist what information is needed to recreate the heapfile.
///
pub(crate) struct HeapFile {
    // TODO milestone hs
    // Add any fields you need to maintain state for a HeapFile

    // Track this HeapFile's container Id
    pub container_id: ContainerId,
    // The following are for profiling/ correctness checks
    pub read_count: AtomicU16,
    pub write_count: AtomicU16,
}

/// HeapFile required functions
impl HeapFile {
    /// Create a new heapfile for the given path. Return Result<Self> if able to create.
    /// Errors could arise from permissions, space, etc when trying to create the file used by HeapFile.
    pub(crate) fn new(file_path: PathBuf, container_id: ContainerId) -> Result<Self, CrustyError> {
        let file = match OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&file_path)
        {
            Ok(f) => f,
            Err(error) => {
                return Err(CrustyError::CrustyError(format!(
                    "Cannot open or create heap file: {} {:?}",
                    file_path.to_string_lossy(),
                    error
                )))
            }
        };

        // TODO milestone hs
        // Initialize the HeapFile

        Ok(HeapFile {
            //TODO milestone hs
            // Add your fields here
            container_id,
            read_count: AtomicU16::new(0),
            write_count: AtomicU16::new(0),
        })
    }

    /// Return the number of pages for this HeapFile.
    /// Return type is PageId (alias for another type) as we cannot have more
    /// pages than PageId can hold.
    pub fn num_pages(&self) -> PageId {
        panic!("TODO milestone hs");
    }

    /// Read the page from the file.
    /// Errors could arise from the filesystem or invalid pageId
    /// Note: that std::io::{Seek, SeekFrom} require Write locks on the underlying std::fs::File
    pub(crate) fn read_page_from_file(&self, pid: PageId) -> Result<Page, CrustyError> {
        //If profiling count reads
        #[cfg(feature = "profile")]
        {
            self.read_count.fetch_add(1, Ordering::Relaxed);
        }
        panic!("TODO milestone hs");
    }

    /// Take a page and write it to the underlying file.
    /// This could be an existing page or a new page
    pub(crate) fn write_page_to_file(&self, page: &Page) -> Result<(), CrustyError> {
        trace!(
            "Writing page {} to file {}",
            page.get_page_id(),
            self.container_id
        );
        //If profiling count writes
        #[cfg(feature = "profile")]
        {
            self.write_count.fetch_add(1, Ordering::Relaxed);
        }
        panic!("TODO milestone hs");
    }
}

#[cfg(test)]
#[allow(unused_must_use)]
mod test {
    use super::*;
    use common::testutil::*;
    use temp_testdir::TempDir;

    #[test]
    fn hs_hf_insert() {
        init();

        //Create a temp file
        let f = gen_random_test_sm_dir();
        let tdir = TempDir::new(f, true);
        let mut f = tdir.to_path_buf();
        f.push(gen_rand_string(4));
        f.set_extension("hf");

        let mut hf = HeapFile::new(f.to_path_buf(), 0).expect("Unable to create HF for test");

        // Make a page and write
        let mut p0 = Page::new(0);
        let bytes = get_random_byte_vec(100);
        p0.add_value(&bytes);
        let bytes = get_random_byte_vec(100);
        p0.add_value(&bytes);
        let bytes = get_random_byte_vec(100);
        p0.add_value(&bytes);
        let p0_bytes = p0.to_bytes();

        hf.write_page_to_file(&p0);
        //check the page
        assert_eq!(1, hf.num_pages());
        let checkp0 = hf.read_page_from_file(0).unwrap();
        assert_eq!(p0_bytes, checkp0.to_bytes());

        //Add another page
        let mut p1 = Page::new(1);
        let bytes = get_random_byte_vec(100);
        p1.add_value(&bytes);
        let bytes = get_random_byte_vec(100);
        p1.add_value(&bytes);
        let bytes = get_random_byte_vec(100);
        p1.add_value(&bytes);
        let p1_bytes = p1.to_bytes();

        hf.write_page_to_file(&p1);

        assert_eq!(2, hf.num_pages());
        //Recheck page0
        let checkp0 = hf.read_page_from_file(0).unwrap();
        assert_eq!(p0_bytes, checkp0.to_bytes());

        //check page 1
        let checkp1 = hf.read_page_from_file(1).unwrap();
        assert_eq!(p1_bytes, checkp1.to_bytes());

        #[cfg(feature = "profile")]
        {
            assert_eq!(*hf.read_count.get_mut(), 3);
            assert_eq!(*hf.write_count.get_mut(), 2);
        }
    }
}
