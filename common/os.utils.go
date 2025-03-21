package common

import "os"

func CreateDirIfNotExists(dir string) error {
	_, err := os.Stat(dir)
	if os.IsNotExist(err) {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return err
		}
	}
	return nil
}

func WriteLinesToFile(lines []string, fileName string) error {

	// Combine the lines into a single byte slice
	data := []byte{}
	for i, line := range lines {
		data = append(data, []byte(line)...) // Add the line
		if i < len(lines)-1 {                // Add \n only if it's not the last line
			data = append(data, '\n')
		}
	}
	// Write to the file
	return os.WriteFile(fileName, data, 0644)
}
