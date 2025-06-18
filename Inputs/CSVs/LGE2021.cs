using XycloneDesigns.Apis.IEC.Tables;

namespace Database.IEC.Inputs.CSVs
{
    public class LGE2021 : CSVRowLGE3
    {
        public LGE2021(string line) : base(line)
        {
            Electoralevent = ElectoralEvent.Types.Municipal + "2021";
        }

        public static bool IsElectoralEvent(ElectoralEvent electoralEvent)
        {
            return
                ElectoralEvent.IsMunicipal(electoralEvent.Type) &&
                ElectoralEvent.IsYear(2021, electoralEvent.Date);
        }
    }
}