using XycloneDesigns.Database.IEC.Tables;

namespace Database.IEC.Inputs.CSVs
{
    public class PE2019 : CSVRowNPE3
    {
        public PE2019(string line) : base(line.Replace(",,", ","))
        {
            Electoralevent = ElectoralEvent.Types.Provincial + "2019";
        }

        public static bool IsElectoralEvent(ElectoralEvent electoralEvent)
        {
            return
                ElectoralEvent.IsProvincial(electoralEvent.Type) &&
                ElectoralEvent.IsYear(2019, electoralEvent.Date);
        }
    }
}