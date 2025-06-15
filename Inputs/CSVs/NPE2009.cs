using XycloneDesigns.Database.IEC.Tables;

namespace Database.IEC.Inputs.CSVs
{
    public class NPE2009 : CSVRowNPE2
    {
        public NPE2009(string line) : base(line) { }

        public static bool IsElectoralEventNE(ElectoralEvent electoralEvent)
        {
            return
                ElectoralEvent.IsNational(electoralEvent.Type) &&
                ElectoralEvent.IsYear(2009, electoralEvent.Date);
        }
        public static bool IsElectoralEventPE(ElectoralEvent electoralEvent)
        {
            return
                ElectoralEvent.IsProvincial(electoralEvent.Type) &&
                ElectoralEvent.IsYear(2009, electoralEvent.Date);
        }
    }
}