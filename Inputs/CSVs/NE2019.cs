using XycloneDesigns.Apis.IEC.Tables;

namespace Database.IEC.Inputs.CSVs
{
    public class NE2019 : CSVRowNPE3
    {
        public NE2019(string line) : base(line)
        {
            Electoralevent = ElectoralEvent.Types.National + "2019";
        }

        public static bool IsElectoralEvent(ElectoralEvent electoralEvent)
        {
            return
                ElectoralEvent.IsNational(electoralEvent.Type) &&
                ElectoralEvent.IsYear(2019, electoralEvent.Date);
        }
    }
}