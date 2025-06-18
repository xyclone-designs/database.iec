using XycloneDesigns.Apis.IEC.Tables;

namespace Database.IEC.Inputs.CSVs
{
    public class NE2024 : CSVRowNPE4
    {
        public NE2024(string line) : base(line.Replace(",,", ","))
        {
            Electoralevent = ElectoralEvent.Types.National + "2024";
        }

        public static bool IsElectoralEvent(ElectoralEvent electoralEvent)
        {
            return
                ElectoralEvent.IsNational(electoralEvent.Type) &&
                ElectoralEvent.IsYear(2024, electoralEvent.Date);
        }
    }
}