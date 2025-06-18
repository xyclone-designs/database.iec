using XycloneDesigns.Apis.IEC.Tables;

namespace Database.IEC.Inputs.CSVs
{
    public class LGE2011 : CSVRowLGE2
    {
        public LGE2011(string line) : base(line) { }

        public static bool IsElectoralEvent(ElectoralEvent electoralEvent)
        {
            return
                ElectoralEvent.IsMunicipal(electoralEvent.Type) &&
                ElectoralEvent.IsYear(2011, electoralEvent.Date);
        }
    }
}