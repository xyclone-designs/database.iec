using XycloneDesigns.Database.IEC.Tables;

namespace Database.IEC.Inputs.CSVs
{
    public class LGE2000 : CSVRowLGE1
    {
        public LGE2000(string line) : base(line) { }

        public static bool IsElectoralEvent(ElectoralEvent electoralEvent)
        {
            return
                ElectoralEvent.IsMunicipal(electoralEvent.Type) &&
                ElectoralEvent.IsYear(2000, electoralEvent.Date);
        }
    }
}