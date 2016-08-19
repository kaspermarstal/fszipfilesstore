FSZipFilesStore
===============

Zip-backed file storage for scrapy FilesPipeline module. Files are compressed in-memory and written directly to a zip-file. A compression ratio of ~16 is achieved with standard HTML/XML files.