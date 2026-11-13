using XycloneDesigns.Apis.IEC.Tables;

namespace Database.IEC.Inputs
{
    public static partial class CSV
    {
        public static class NPE
        {
            public class _1999 : RowNPE1
            {
                public _1999(string line) : base(line)
                {
                    MunicipalityGeo = null;
                    MunicipalityName = MunicipalityName?.Trim('"', ' ');
                }

                public static bool IsElectoralEventNE(ElectoralEvent electoralEvent)
                {
                    return
                        XycloneDesigns.Apis.IEC.Tables.ElectoralEvent.IsNational(electoralEvent.Type) &&
                        XycloneDesigns.Apis.IEC.Tables.ElectoralEvent.IsYear(1999, electoralEvent.Date);
                }
                public static bool IsElectoralEventPE(ElectoralEvent electoralEvent)
                {
                    return
                        XycloneDesigns.Apis.IEC.Tables.ElectoralEvent.IsProvincial(electoralEvent.Type) &&
                        XycloneDesigns.Apis.IEC.Tables.ElectoralEvent.IsYear(1999, electoralEvent.Date);
                }
            }
            public class _2004(string line) : RowNPE1(line)
            {
                public static bool IsElectoralEventNE(ElectoralEvent electoralEvent)
                {
                    return
                        XycloneDesigns.Apis.IEC.Tables.ElectoralEvent.IsNational(electoralEvent.Type) &&
                        XycloneDesigns.Apis.IEC.Tables.ElectoralEvent.IsYear(2004, electoralEvent.Date);
                }
                public static bool IsElectoralEventPE(ElectoralEvent electoralEvent)
                {
                    return
                        XycloneDesigns.Apis.IEC.Tables.ElectoralEvent.IsProvincial(electoralEvent.Type) &&
                        XycloneDesigns.Apis.IEC.Tables.ElectoralEvent.IsYear(2004, electoralEvent.Date);
                }
            }
            public class _2009(string line) : RowNPE2(line)
            {
                public static bool IsElectoralEventNE(ElectoralEvent electoralEvent)
                {
                    return
                        XycloneDesigns.Apis.IEC.Tables.ElectoralEvent.IsNational(electoralEvent.Type) &&
                        XycloneDesigns.Apis.IEC.Tables.ElectoralEvent.IsYear(2009, electoralEvent.Date);
                }
                public static bool IsElectoralEventPE(ElectoralEvent electoralEvent)
                {
                    return
                        XycloneDesigns.Apis.IEC.Tables.ElectoralEvent.IsProvincial(electoralEvent.Type) &&
                        XycloneDesigns.Apis.IEC.Tables.ElectoralEvent.IsYear(2009, electoralEvent.Date);
                }
            }
            public class _2014(string line) : RowNPE2(line)
            {
                public static bool IsElectoralEventNE(ElectoralEvent electoralEvent)
                {
                    return
                        XycloneDesigns.Apis.IEC.Tables.ElectoralEvent.IsNational(electoralEvent.Type) &&
                        XycloneDesigns.Apis.IEC.Tables.ElectoralEvent.IsYear(2014, electoralEvent.Date);
                }
                public static bool IsElectoralEventPE(ElectoralEvent electoralEvent)
                {
                    return
                        XycloneDesigns.Apis.IEC.Tables.ElectoralEvent.IsProvincial(electoralEvent.Type) &&
                        XycloneDesigns.Apis.IEC.Tables.ElectoralEvent.IsYear(2014, electoralEvent.Date);
                }
            }
        }
    }
}