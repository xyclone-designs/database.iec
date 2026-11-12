using XycloneDesigns.Apis.IEC.Tables;

namespace Database.IEC.Inputs.CSVs
{
    public class LGE2026 : CSVRowLGE3
    {
        public LGE2026(string line) : base(line)
        {
            Electoralevent = ElectoralEvent.Types.Municipal + "2026";
        }

        public static bool IsElectoralEvent(ElectoralEvent electoralEvent)
        {
            return
                ElectoralEvent.IsMunicipal(electoralEvent.Type) &&
                ElectoralEvent.IsYear(2026, electoralEvent.Date);
        }
    }
}