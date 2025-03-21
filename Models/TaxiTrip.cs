using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TaxiDataApp.Models
{
    public class TaxiTrip
    {
        public string OriginalPickupDateTime { get; set; }//for duplicates
        public string OriginalDropoffDateTime { get; set; }

        // filds from table TaxiTrips
        public DateTime tpep_pickup_datetime { get; set; }
        public DateTime tpep_dropoff_datetime { get; set; }
        public int passenger_count { get; set; }
        public float trip_distance { get; set; }
        public string store_and_fwd_flag { get; set; }
        public int PULocationID { get; set; }
        public int DOLocationID { get; set; }
        public decimal fare_amount { get; set; }
        public decimal tip_amount { get; set; }
    }
}
