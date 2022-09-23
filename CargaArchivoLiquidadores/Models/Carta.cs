namespace CargaArchivoLiquidadores
{
    public partial class Carta
    {
        public string Proveedor { get; set; }
        public string NumeroSolicitud { get; set; }
        public string NumeroCarta { get; set; }
        public string FechaCarta { get; set; }
        public string CodigoPrestacion { get; set; }
        public string CantidadDocumentosDevueltos { get; set; }
        public string CodigoMotivo { get; set; }
        public string DescripcionMotivo { get; set; }
        public string Glosa { get; set; }
        public string EstadoCarta { get; set; }
        public string IndicadorEnvio { get; set; }
        public string Liquidador { get; set; }
        public string DireccionPaciente { get; set; }
        public string MontoTotal { get; set; }
        public string FechaExtraccion { get; set; }
    }
}
