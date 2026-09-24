package jr_requester

import (
	"errors"
	"os"
	"sync"
)

// ------------
// Errors
// ------------

var (
	ErrEmptyPath   = errors.New("path cannot be empty")
	ErrDevNullPath = errors.New("path cannot be os.DevNull")
	ErrInvalidPath = errors.New("invalid path")
)

// ------------
// Structs
// ------------

type IConnPersistency interface{}

type ConnPersistency struct {
	cachedPaths map[string]*os.File
	mu          sync.Mutex
}

// ------------
// Methods
// ------------

// NewConnPersistency creates a new instance of ConnPersistency with the provided writer and reader.
func NewConnPersistency() *ConnPersistency {
	return &ConnPersistency{
		cachedPaths: make(map[string]*os.File),
	}
}

// Path Handling

// validatePath checks if the provided path is valid. If createIfNotExist is true, it will create the directory if it doesn't exist.
// If closeAfterValidation is true, it will close the file after validation.
func (c *ConnPersistency) validatePath(path string, createIfNotExist bool, closeAfterValidation bool) (*os.File, error) {
	if path == "" {
		return nil, ErrorFormat(ErrEmptyPath, "path cannot be empty", "")
	}
	if path == os.DevNull {
		return nil, ErrorFormat(ErrDevNullPath, "path cannot be os.DevNull", "")
	}

	if _, err := os.Stat(path); os.IsNotExist(err) {
		if createIfNotExist {
			if err := os.MkdirAll(path, 0755); err != nil {
				return nil, ErrorFormat(ErrInvalidPath, "failed to create directory", "")
			}
		} else {
			return nil, ErrorFormat(ErrInvalidPath, "directory does not exist", "")
		}
	}

	filePath, err := os.Open(path)
	if err != nil {
		return nil, ErrorFormat(ErrInvalidPath, "failed to open file", "")
	}
	if closeAfterValidation {
		defer filePath.Close() // Ensure the file is closed after validation
	}
	return filePath, nil
}

// AddCachedPath adds a path to the cached paths map after validating it. If createIfNotExist is true, it will create the directory if it doesn't exist.
func (c *ConnPersistency) AddCachedPath(path string, createIfNotExist bool, mode os.FileMode) (*os.File, error) {
	filePath, err := c.validatePath(path, createIfNotExist, true)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cachedPaths[path] = filePath
	if mode != 0 {
		if err := os.Chmod(path, mode); err != nil {
			return nil, ErrorFormat(ErrInvalidPath, "failed to set file mode", "")
		}
	}
	return filePath, nil
}

// RemoveCachedPath removes a path from the cached paths map.
func (c *ConnPersistency) RemoveCachedPath(path string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.cachedPaths, path)
}

// GetCachedPath retrieves a cached path from the map. It returns the file and a boolean indicating if the path was found.
func (c *ConnPersistency) GetCachedPath(path string) (*os.File, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	file, exists := c.cachedPaths[path]
	return file, exists
}

// GetListOfCachedPaths returns a slice of all cached paths.
func (c *ConnPersistency) GetListOfCachedPaths() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	paths := make([]string, 0, len(c.cachedPaths))
	for path := range c.cachedPaths {
		paths = append(paths, path)
	}
	return paths
}

// CloseAllCachedPaths closes all cached files and clears the cached paths map.
func (c *ConnPersistency) CloseAllCachedPaths() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for path, file := range c.cachedPaths {
		file.Close()
		delete(c.cachedPaths, path)
	}
}

// CloseCachedPath closes a specific cached file and removes it from the cached paths map.
func (c *ConnPersistency) CloseCachedPath(path string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	file, exists := c.cachedPaths[path]
	if !exists {
		return ErrorFormat(ErrInvalidPath, "path not found in cached paths", "")
	}
	file.Close()
	delete(c.cachedPaths, path)
	return nil
}

// IsCachedPath checks if a given path is in the cached paths map.
func (c *ConnPersistency) IsCachedPath(path string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, exists := c.cachedPaths[path]
	return exists
}

// Save methods

// Save writes data to all cached paths. It returns an error if writing to any path fails.
func (c *ConnPersistency) Save(data []byte) error {
	for path, file := range c.cachedPaths {
		_, err := file.Write(data)
		if err != nil {
			return ErrorFormat(err, "failed to write data to cached path", "path: %s", path)
		}
	}
	return nil
}

// SaveToPath writes data to a specific cached path. If the path is not cached,
// it validates and adds it to the cached paths map before writing. It returns an error if writing fails.
func (c *ConnPersistency) SaveToPath(data []byte, path string, mode os.FileMode) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	file, exists := c.cachedPaths[path]
	if !exists {
		// Make path if it doesn't exist
		var err error
		filePath, err := c.validatePath(path, true, false)
		if err != nil {
			return ErrorFormat(err, "failed to validate path", "path: %s", path)
		}
		c.cachedPaths[path] = filePath
		file = filePath
		// Set the file mode
		err = os.Chmod(file.Name(), mode)
		if err != nil {
			return ErrorFormat(err, "failed to set file mode", "path: %s", path)
		}
	}
	_, err := file.Write(data)
	if err != nil {
		return ErrorFormat(err, "failed to write data to cached path", "path: %s", path)
	}
	return nil
}
