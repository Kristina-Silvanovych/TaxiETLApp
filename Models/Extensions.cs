using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;

namespace TaxiDataApp.Models
{
    //for Bulk insertion
    public static class Extensions
    {
        public static DataTable AsDataTable(this IEnumerable<TaxiTrip> data)
        {
            var table = new DataTable();
            table.Columns.Add("tpep_pickup_datetime", typeof(DateTime));
            table.Columns.Add("tpep_dropoff_datetime", typeof(DateTime));
            table.Columns.Add("passenger_count", typeof(int));
            table.Columns.Add("trip_distance", typeof(float));
            table.Columns.Add("store_and_fwd_flag", typeof(string));
            table.Columns.Add("PULocationID", typeof(int));
            table.Columns.Add("DOLocationID", typeof(int));
            table.Columns.Add("fare_amount", typeof(decimal));
            table.Columns.Add("tip_amount", typeof(decimal));

            foreach (var item in data)
            {
                table.Rows.Add(item.tpep_pickup_datetime, item.tpep_dropoff_datetime, item.passenger_count,
                    item.trip_distance, item.store_and_fwd_flag, item.PULocationID, item.DOLocationID,
                    item.fare_amount, item.tip_amount);
            }

            return table;
        }
    }
}
