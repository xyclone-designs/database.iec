using XycloneDesigns.Database.IEC.Tables;

namespace Database.IEC.Inputs.CSVs
{
    public class LGE2016 : CSVRowLGE3
    {
        public LGE2016(string line) : base(line) 
        {
            Electoralevent = ElectoralEvent.Types.Municipal + "2016";
        }

        public static bool IsElectoralEvent(ElectoralEvent electoralEvent)
        {
            return
                ElectoralEvent.IsMunicipal(electoralEvent.Type) &&
                ElectoralEvent.IsYear(2016, electoralEvent.Date);
        }
    }
}