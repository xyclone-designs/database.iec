using XycloneDesigns.Apis.IEC.Tables;

namespace Database.IEC.Inputs
{
    public static partial class CSV
    {
        public static class RE
        {
            public class _2024 : RowNPE4
            {
                public _2024(string line) : base(line.Replace(",,", ","))
                {
                    ElectoralEvent = XycloneDesigns.Apis.IEC.Tables.ElectoralEvent.Types.Regional + "2024";
                }

                public static bool IsElectoralEvent(ElectoralEvent electoralEvent)
                {
                    return
                        XycloneDesigns.Apis.IEC.Tables.ElectoralEvent.IsRegional(electoralEvent.Type) &&
                        XycloneDesigns.Apis.IEC.Tables.ElectoralEvent.IsYear(2024, electoralEvent.Date);
                }
            }
        }
    }
}