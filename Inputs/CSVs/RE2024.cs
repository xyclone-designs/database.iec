using XycloneDesigns.Apis.IEC.Tables;

namespace Database.IEC.Inputs.CSVs
{
    public class RE2024 : CSVRowNPE4
    {
        public RE2024(string line) : base(line.Replace(",,", ","))
        {
            Electoralevent = ElectoralEvent.Types.Regional + "2024";
        }

        public static bool IsElectoralEvent(ElectoralEvent electoralEvent)
        {
            return
                ElectoralEvent.IsRegional(electoralEvent.Type) &&
                ElectoralEvent.IsYear(2024, electoralEvent.Date);
        }
    }
}