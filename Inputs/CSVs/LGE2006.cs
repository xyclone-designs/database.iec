using XycloneDesigns.Database.IEC.Tables;

namespace Database.IEC.Inputs.CSVs
{
    public class LGE2006 : CSVRowLGE2
    {
        public LGE2006(string line) : base(line) { }

        public static bool IsElectoralEvent(ElectoralEvent electoralEvent)
        {
            return
                ElectoralEvent.IsMunicipal(electoralEvent.Type) &&
                ElectoralEvent.IsYear(2006, electoralEvent.Date);
        }
    }
}