using XycloneDesigns.Database.IEC.Tables;

namespace Database.IEC.Inputs.CSVs
{
    public class PE2024 : CSVRowNPE4
    {
        public PE2024(string line) : base(line.Replace(",,", ","))
        {
            Electoralevent = ElectoralEvent.Types.Provincial + "2024";
        }

        public static bool IsElectoralEvent(ElectoralEvent electoralEvent)
        {
            return
                ElectoralEvent.IsProvincial(electoralEvent.Type) &&
                ElectoralEvent.IsYear(2024, electoralEvent.Date);
        }
    }
}