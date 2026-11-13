using XycloneDesigns.Apis.IEC.Tables;

namespace Database.IEC.Inputs
{
    public static partial class CSV
    {
        public static class LGE
        {
            public class _2000(string line) : RowLGE1(line)
            {
                public static bool IsElectoralEvent(ElectoralEvent electoralEvent)
                {
                    return
                        XycloneDesigns.Apis.IEC.Tables.ElectoralEvent.IsMunicipal(electoralEvent.Type) &&
                        XycloneDesigns.Apis.IEC.Tables.ElectoralEvent.IsYear(2000, electoralEvent.Date);
                }
            }
            public class _2006(string line) : RowLGE2(line)
            {
                public static bool IsElectoralEvent(ElectoralEvent electoralEvent)
                {
                    return
                        XycloneDesigns.Apis.IEC.Tables.ElectoralEvent.IsMunicipal(electoralEvent.Type) &&
                        XycloneDesigns.Apis.IEC.Tables.ElectoralEvent.IsYear(2006, electoralEvent.Date);
                }
            }
            public class _2011(string line) : RowLGE2(line)
            {
                public static bool IsElectoralEvent(ElectoralEvent electoralEvent)
                {
                    return
                        XycloneDesigns.Apis.IEC.Tables.ElectoralEvent.IsMunicipal(electoralEvent.Type) &&
                        XycloneDesigns.Apis.IEC.Tables.ElectoralEvent.IsYear(2011, electoralEvent.Date);
                }
            }
            public class _2016 : RowLGE3
            {
                public _2016(string line) : base(line)
                {
                    ElectoralEvent = XycloneDesigns.Apis.IEC.Tables.ElectoralEvent.Types.Municipal + "2016";
                }

                public static bool IsElectoralEvent(ElectoralEvent electoralEvent)
                {
                    return
                        XycloneDesigns.Apis.IEC.Tables.ElectoralEvent.IsMunicipal(electoralEvent.Type) &&
                        XycloneDesigns.Apis.IEC.Tables.ElectoralEvent.IsYear(2016, electoralEvent.Date);
                }
            }
            public class _2021 : RowLGE3
            {
                public _2021(string line) : base(line)
                {
                    ElectoralEvent = XycloneDesigns.Apis.IEC.Tables.ElectoralEvent.Types.Municipal + "2021";
                }

                public static bool IsElectoralEvent(ElectoralEvent electoralEvent)
                {
                    return
                        XycloneDesigns.Apis.IEC.Tables.ElectoralEvent.IsMunicipal(electoralEvent.Type) &&
                        XycloneDesigns.Apis.IEC.Tables.ElectoralEvent.IsYear(2021, electoralEvent.Date);
                }
            }
            public class _2026 : RowLGE3
            {
                public _2026(string line) : base(line)
                {
                    ElectoralEvent = XycloneDesigns.Apis.IEC.Tables.ElectoralEvent.Types.Municipal + "2026";
                }

                public static bool IsElectoralEvent(ElectoralEvent electoralEvent)
                {
                    return
                        XycloneDesigns.Apis.IEC.Tables.ElectoralEvent.IsMunicipal(electoralEvent.Type) &&
                        XycloneDesigns.Apis.IEC.Tables.ElectoralEvent.IsYear(2026, electoralEvent.Date);
                }
            }
        }
    }
}