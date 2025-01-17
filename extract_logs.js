const fs = require('fs');
const readline = require('readline');
const path = require('path');

class LogExtractor {
    constructor(logFilePath) {
        this.logFilePath = logFilePath;
        this.outputDir = path.join(__dirname, 'output');
        this.fileSize = 0;
    }

    /**
     * Generates sample log data for testing
     * @param {number} numberOfDays - Number of days to generate logs for
     * @param {number} entriesPerDay - Number of log entries per day
     */
    async generateSampleData(numberOfDays = 10, entriesPerDay = 1000) {
        const logLevels = ['INFO', 'WARN', 'ERROR', 'DEBUG'];
        const messages = [
            'User logged in',
            'Failed to connect to the database',
            'Disk space running low',
            'Cache cleared',
            'Request processed successfully'
        ];

        const writeStream = fs.createWriteStream(this.logFilePath);
        const startDate = new Date('2024-12-01');

        for (let day = 0; day < numberOfDays; day++) {
            const currentDate = new Date(startDate);
            currentDate.setDate(startDate.getDate() + day);
            
            for (let entry = 0; entry < entriesPerDay; entry++) {
                const hour = Math.floor(Math.random() * 24);
                const minute = Math.floor(Math.random() * 60);
                const second = Math.floor(Math.random() * 60);
                const logLevel = logLevels[Math.floor(Math.random() * logLevels.length)];
                const message = messages[Math.floor(Math.random() * messages.length)];

                const timestamp = `${currentDate.toISOString().split('T')[0]} ` +
                    `${hour.toString().padStart(2, '0')}:` +
                    `${minute.toString().padStart(2, '0')}:` +
                    `${second.toString().padStart(2, '0')}`;

                const logEntry = `${timestamp} ${logLevel} ${message}\n`;
                writeStream.write(logEntry);
            }
        }

        await new Promise(resolve => writeStream.end(resolve));
        console.log('Sample log data generated successfully.');
    }

    /**
     * Gets the date from a specific position in file
     * @param {number} position - File position
     * @returns {Promise<string>} - Date in YYYY-MM-DD format
     */
    async getDateAtPosition(position) {
        return new Promise((resolve, reject) => {
            const stream = fs.createReadStream(this.logFilePath, {
                start: position,
                encoding: 'utf8'
            });

            let data = '';
            stream.on('data', chunk => {
                data += chunk;
                const newlineIndex = data.indexOf('\n');
                if (newlineIndex !== -1) {
                    stream.destroy();
                    const line = data.slice(0, newlineIndex);
                    const date = line.substring(0, 10);
                    resolve(date);
                }
            });

            stream.on('error', reject);
        });
    }

    /**
     * Performs binary search to find the start position of a date
     * @param {string} targetDate - Date to search for
     * @returns {Promise<number>} - File position where date starts
     */
    async binarySearch(targetDate) {
        let left = 0;
        let right = this.fileSize - 1;
        let startPosition = -1;

        while (left <= right) {
            const mid = Math.floor(left + (right - left) / 2);
            const date = await this.getDateAtPosition(mid);

            if (date === targetDate) {
                // Found a match, but need to find the first occurrence
                startPosition = mid;
                right = mid - 1;
            } else if (date < targetDate) {
                left = mid + 1;
            } else {
                right = mid - 1;
            }
        }

        // If we found a position, backtrack to find the first occurrence
        if (startPosition !== -1) {
            while (startPosition > 0) {
                const prevDate = await this.getDateAtPosition(startPosition - 1);
                if (prevDate !== targetDate) break;
                startPosition--;
            }
        }

        return startPosition;
    }

    /**
     * Ensures output directory exists
     */
    async initializeOutputDir() {
        if (!fs.existsSync(this.outputDir)) {
            await fs.promises.mkdir(this.outputDir, { recursive: true });
        }
    }

    /**
     * Extracts logs for a specific date using binary search
     * @param {string} targetDate - Date in YYYY-MM-DD format
     * @returns {Promise<{totalLines: number, matchedLines: number, executionTime: number}>}
     */
    async extractLogs(targetDate) {
        const startTime = Date.now();
        let matchedLines = 0;

        // Validate date format
        if (!/^\d{4}-\d{2}-\d{2}$/.test(targetDate)) {
            throw new Error('Invalid date format. Please use YYYY-MM-DD');
        }

        await this.initializeOutputDir();
        const outputPath = path.join(this.outputDir, `output_${targetDate}.txt`);
        const outputStream = fs.createWriteStream(outputPath);

        // Get file size for binary search
        const stats = await fs.promises.stat(this.logFilePath);
        this.fileSize = stats.size;

        // Find the starting position using binary search
        const startPosition = await this.binarySearch(targetDate);
        
        if (startPosition === -1) {
            console.log(`No entries found for date: ${targetDate}`);
            return { totalLines: 0, matchedLines: 0, executionTime: Date.now() - startTime };
        }

        // Read from the found position
        const fileStream = fs.createReadStream(this.logFilePath, {
            start: startPosition,
            encoding: 'utf8',
            highWaterMark: 64 * 1024
        });

        const rl = readline.createInterface({
            input: fileStream,
            crlfDelay: Infinity
        });

        try {
            for await (const line of rl) {
                const lineDate = line.substring(0, 10);
                
                if (lineDate !== targetDate) {
                    break; // We've passed all entries for this date
                }

                await new Promise((resolve, reject) => {
                    outputStream.write(line + '\n', (error) => {
                        if (error) reject(error);
                        else resolve();
                    });
                });
                matchedLines++;
            }
        } finally {
            outputStream.end();
            rl.close();
        }

        const executionTime = Date.now() - startTime;

        return {
            totalLines: this.fileSize, // Approximate total lines
            matchedLines,
            executionTime
        };
    }
}

/**
 * Main execution function
 */
async function main() {
    try {
        const targetDate = process.argv[2];
        if (!targetDate) {
            console.error('Please provide a date in YYYY-MM-DD format');
            process.exit(1);
        }

        const logFilePath = path.join(__dirname, 'test_logs.log');
        const extractor = new LogExtractor(logFilePath);

        // Generate sample data if the log file doesn't exist
        if (!fs.existsSync(logFilePath)) {
            console.log('Generating sample log data...');
            await extractor.generateSampleData();
        }

        console.log(`Starting log extraction for date: ${targetDate}`);
        const result = await extractor.extractLogs(targetDate);

        console.log('\nExtraction completed successfully!');
        console.log(`Matching lines found: ${result.matchedLines.toLocaleString()}`);
        console.log(`Execution time: ${(result.executionTime / 1000).toFixed(2)} seconds`);
        console.log(`Output saved to: ${path.join('output', `output_${targetDate}.txt`)}`);

    } catch (error) {
        console.error('Error during log extraction:', error.message);
        process.exit(1);
    }
}

// Execute if running directly
if (require.main === module) {
    main();
}

module.exports = LogExtractor;