using System;
using System.Collections;
using System.Data;
using System.Linq;
using System.Net;
using ClickHouse.Ado.Impl.ATG.Insert;

namespace ClickHouse.Ado.Impl.ColumnTypes
{
    internal class IPv4ColumnType : ColumnType
    {
        public IPAddress[] Data { get; private set; }
        public override int Rows => Data?.Length ?? 0;

        public IPv4ColumnType()
        {
            
        }
        
        public IPv4ColumnType(IPAddress[] data)
        {
            Data = data;
        }
        
        internal override void Read(ProtocolFormatter formatter, int rows)
        {
            Data = new IPAddress[rows];
            for (var i = 0; i < rows; i++)
            {
                Data[i] = new IPAddress(formatter.ReadBytes(4).Reverse().ToArray());
            }
        }

        internal override Type CLRType => typeof(IPAddress);
        
        public override void ValueFromConst(Parser.ValueType val)
        {
            switch (val.TypeHint)
            {
                case Parser.ConstType.String:
                    Data = new[] { IPAddress.Parse(val.StringValue) };
                    break;
                case Parser.ConstType.Number:
                    byte[] rawIPAddress = BitConverter.GetBytes(Convert.ToUInt32(val.StringValue)).Reverse().ToArray();
                    Data = new[] { new IPAddress(rawIPAddress) };
                    break;
                default:
                    throw new NotSupportedException();
            }
        }

        public override string AsClickHouseType()
        {
            return "IPv4";
        }

        public override void Write(ProtocolFormatter formatter, int rows)
        {
            foreach (var d in Data)
            {
                formatter.WriteBytes(d.GetAddressBytes().Reverse().ToArray());
            }
        }

        public override void ValueFromParam(ClickHouseParameter parameter)
        {
            switch (parameter.DbType)
            {
                case DbType.String:
                case DbType.AnsiString:    
                    Data = new[] { IPAddress.Parse(parameter.Value.ToString()) };
                    break;
                case DbType.UInt32:
                    byte[] rawIPAddress = BitConverter.GetBytes(Convert.ToUInt32(parameter.Value)).Reverse().ToArray();
                    Data = new[] { new IPAddress(rawIPAddress) };
                    break;
                default:
                    throw new InvalidCastException($"Cannot convert parameter with type {parameter.DbType} to {typeof(IPAddress).Name}.");
            }
        }

        public override object Value(int currentRow)
        {
            return Data[currentRow];
        }

        public override long IntValue(int currentRow)
        {
            return BitConverter.ToUInt32(Data[currentRow].GetAddressBytes(), 0);
        }

        public override void ValuesFromConst(IEnumerable objects)
        {
            Data = objects.Cast<IPAddress>().ToArray();
        }
    }
}