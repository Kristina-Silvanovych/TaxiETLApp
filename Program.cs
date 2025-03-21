using CsvHelper.Configuration;
using CsvHelper;
using Microsoft.Data.SqlClient;
using System.Globalization;
using TaxiDataApp.Models;
using System.Collections.Generic;
using System.IO;

namespace TaxiDataApp
{
    static class Program
    {
        static string connectionString = "Data Source=(localdb)\\MSSQLLocalDB;Initial Catalog=TaxiData;Integrated Security=True;Encrypt=True";
        static string csvPath = "Data/sample-cab-data.csv";
        static string duplicatesPath = "duplicates.csv";

        static void Main(string[] args)
        {
            try
            {
                //The existence of the input file check
                if (!File.Exists(csvPath))
                {
                    Console.WriteLine($"Error: Input file {csvPath} does not exist.");
                    return;
                }

                //The possibility of writing duplicates to the file check
                try
                {
                    using (var testWriter = new StreamWriter(duplicatesPath, false))
                    {
                        testWriter.Write("");
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error: Cannot write to duplicates file {duplicatesPath}. {ex.Message}");
                    return;
                }

                //Extracting and transformation
                var (records, duplicates) = ExtractAndTransform(csvPath);

                //Loading in DB
                LoadToDatabase(records);

                //Duplicates saving
                SaveDuplicates(duplicates, duplicatesPath);

                //Counting of rows
                int rowCount = GetRowCount();
                Console.WriteLine($"ETL completed. Rows in table: {rowCount}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error: {ex.Message}");
            }
        }

        static (List<TaxiTrip> records, List<TaxiTrip> duplicates) ExtractAndTransform(string filePath)
        {
            var records = new List<TaxiTrip>();
            var duplicates = new List<TaxiTrip>();
            var seenKeys = new HashSet<string>();

            var config = new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                HasHeaderRecord = true,
                TrimOptions = TrimOptions.Trim,
                MissingFieldFound = null
            };

            try
            {
                using (var reader = new StreamReader(filePath))
                using (var csv = new CsvReader(reader, config))
                {
                    csv.Read();
                    csv.ReadHeader();
                    int rowNumber = 0;

                    while (csv.Read())
                    {
                        rowNumber++;
                        try
                        {
                            var record = new TaxiTrip
                            {
                                OriginalPickupDateTime = csv.GetField("tpep_pickup_datetime") ?? throw new InvalidOperationException($"Missing tpep_pickup_datetime in row {rowNumber}"),
                                OriginalDropoffDateTime = csv.GetField("tpep_dropoff_datetime") ?? throw new InvalidOperationException($"Missing tpep_dropoff_datetime in row {rowNumber}"),
                                passenger_count = int.TryParse(csv.GetField("passenger_count"), out int pc) ? pc : 0,
                                trip_distance = float.TryParse(csv.GetField("trip_distance"), NumberStyles.Any, CultureInfo.InvariantCulture, out float td) ? td : 0,
                                store_and_fwd_flag = csv.GetField("store_and_fwd_flag") == "N" ? "No" : "Yes",
                                PULocationID = int.TryParse(csv.GetField("PULocationID"), out int pu) ? pu : 0,
                                DOLocationID = int.TryParse(csv.GetField("DOLocationID"), out int doLoc) ? doLoc : 0,
                                fare_amount = decimal.TryParse(csv.GetField("fare_amount"), NumberStyles.Any, CultureInfo.InvariantCulture, out decimal fa) && fa >= 0 ? fa : 0, 
                                tip_amount = decimal.TryParse(csv.GetField("tip_amount"), NumberStyles.Any, CultureInfo.InvariantCulture, out decimal ta) && ta >= 0 ? ta : 0 
                            };

                            //Additional validation
                            if (record.passenger_count < 0 || record.passenger_count > 10)
                                throw new InvalidOperationException($"Invalid passenger_count in row {rowNumber}: {record.passenger_count}");
                            if (record.trip_distance < 0)
                                throw new InvalidOperationException($"Invalid trip_distance in row {rowNumber}: {record.trip_distance}");
                            if (record.trip_distance > 1000)
                                throw new InvalidOperationException($"Trip distance exceeds 1000 miles in row {rowNumber}: {record.trip_distance}");
                            if (record.PULocationID <= 0)
                                throw new InvalidOperationException($"Invalid PULocationID in row {rowNumber}: {record.PULocationID}");
                            if (record.PULocationID > 266)
                                throw new InvalidOperationException($"PULocationID exceeds maximum value (263) in row {rowNumber}: {record.PULocationID}");
                            if (record.DOLocationID <= 0)
                                throw new InvalidOperationException($"Invalid DOLocationID in row {rowNumber}: {record.DOLocationID}");
                            if (record.DOLocationID > 266)
                                throw new InvalidOperationException($"DOLocationID exceeds maximum value (263) in row {rowNumber}: {record.DOLocationID}");
                            if (record.fare_amount > 500)
                                throw new InvalidOperationException($"Fare amount exceeds $500 in row {rowNumber}: {record.fare_amount}");
                            if (record.tip_amount > 100)
                                throw new InvalidOperationException($"Tip amount exceeds $100 in row {rowNumber}: {record.tip_amount}");        

                            //Converting from EST to UTC
                            record.tpep_pickup_datetime = ConvertToUtc(record.OriginalPickupDateTime, rowNumber);
                            record.tpep_dropoff_datetime = ConvertToUtc(record.OriginalDropoffDateTime, rowNumber);

                            //Time check
                            if (record.tpep_dropoff_datetime < record.tpep_pickup_datetime)
                                throw new InvalidOperationException($"Dropoff time is earlier than pickup time in row {rowNumber}");

                            //Trip duration check
                            TimeSpan tripDuration = record.tpep_dropoff_datetime - record.tpep_pickup_datetime;
                            if (tripDuration.TotalHours > 24)
                                throw new InvalidOperationException($"Trip duration exceeds 24 hours in row {rowNumber}: {tripDuration.TotalHours} hours");

                            //Date check
                            DateTime minDate = new DateTime(2000, 1, 1);
                            if (record.tpep_pickup_datetime < minDate || record.tpep_pickup_datetime > DateTime.UtcNow)
                                throw new InvalidOperationException($"Pickup date/time is out of range in row {rowNumber}: {record.tpep_pickup_datetime}");
                            if (record.tpep_dropoff_datetime < minDate || record.tpep_dropoff_datetime > DateTime.UtcNow)
                                throw new InvalidOperationException($"Dropoff date/time is out of range in row {rowNumber}: {record.tpep_dropoff_datetime}");

                            //Duplicates check
                            string key = $"{record.OriginalPickupDateTime}|{record.OriginalDropoffDateTime}|{record.passenger_count}";
                            if (!seenKeys.Add(key))
                            {
                                duplicates.Add(record);
                            }
                            else
                            {
                                records.Add(record);
                            }
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"Error processing row {rowNumber}: {ex.Message}");
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error reading CSV file {filePath}: {ex.Message}");
                return (records, duplicates);
            }

            return (records, duplicates);
        }

        static DateTime ConvertToUtc(string estDateTime, int rowNumber)
        {
            try
            {
                DateTime localTime = DateTime.ParseExact(estDateTime, "MM/dd/yyyy hh:mm:ss tt", CultureInfo.InvariantCulture);
                TimeZoneInfo est = TimeZoneInfo.FindSystemTimeZoneById("Eastern Standard Time");
                return TimeZoneInfo.ConvertTimeToUtc(localTime, est);
            }
            catch (FormatException ex)
            {
                throw new FormatException($"Invalid date/time format in row {rowNumber}: {estDateTime}. {ex.Message}", ex);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Error converting date/time in row {rowNumber}: {estDateTime}. {ex.Message}", ex);
            }
        }

        static void LoadToDatabase(List<TaxiTrip> records)
        {
            try
            {
                using (var connection = new SqlConnection(connectionString))
                {
                    connection.Open();
                    using (var bulkCopy = new SqlBulkCopy(connection))
                    {
                        bulkCopy.DestinationTableName = "TaxiTrips";
                        bulkCopy.ColumnMappings.Add("tpep_pickup_datetime", "tpep_pickup_datetime");
                        bulkCopy.ColumnMappings.Add("tpep_dropoff_datetime", "tpep_dropoff_datetime");
                        bulkCopy.ColumnMappings.Add("passenger_count", "passenger_count");
                        bulkCopy.ColumnMappings.Add("trip_distance", "trip_distance");
                        bulkCopy.ColumnMappings.Add("store_and_fwd_flag", "store_and_fwd_flag");
                        bulkCopy.ColumnMappings.Add("PULocationID", "PULocationID");
                        bulkCopy.ColumnMappings.Add("DOLocationID", "DOLocationID");
                        bulkCopy.ColumnMappings.Add("fare_amount", "fare_amount");
                        bulkCopy.ColumnMappings.Add("tip_amount", "tip_amount");

                        bulkCopy.WriteToServer(records.AsDataTable());
                    }
                }
            }
            catch (SqlException ex)
            {
                throw new InvalidOperationException($"Database error: {ex.Message}", ex);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Error loading data to database: {ex.Message}", ex);
            }
        }

        static void SaveDuplicates(List<TaxiTrip> duplicates, string filePath)
        {
            try
            {
                using (var writer = new StreamWriter(filePath))
                using (var csv = new CsvWriter(writer, CultureInfo.InvariantCulture))
                {
                    csv.WriteRecords(duplicates);
                }
                Console.WriteLine($"Duplicates saved to {filePath}");
            }
            catch (IOException ex)
            {
                throw new IOException($"Error saving duplicates to {filePath}: {ex.Message}", ex);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Error saving duplicates to {filePath}: {ex.Message}", ex);
            }
        }

        static int GetRowCount()
        {
            try
            {
                using (var connection = new SqlConnection(connectionString))
                {
                    connection.Open();
                    using (var command = new SqlCommand("SELECT COUNT(*) FROM TaxiTrips", connection))
                    {
                        return (int)command.ExecuteScalar();
                    }
                }
            }
            catch (SqlException ex)
            {
                Console.WriteLine($"Error counting rows in database: {ex.Message}");
                return 0;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error counting rows: {ex.Message}");
                return 0;
            }
        }
    }
}
    
