using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Dynamic;
using System.Globalization;
using System.Linq;
using System.Text;
using DynamicParameters = SpMapper4Dapper.Dapper.DynamicParameters;
using SqlMapper = SpMapper4Dapper.Dapper.SqlMapper;

namespace SpMapper4Dapper
{
	/// <summary>
	/// The dal helper.
	/// </summary>
	public static class DalHelper
	{
		#region Private members

		/// <summary>
		/// The type map.
		/// </summary>
		private static readonly Dictionary<Type, DbType> TypeMap;
		#endregion

		#region Constructor

		/// <summary>
		/// Initializes static members of the <see cref="DalHelper"/> class.
		/// </summary>
		static DalHelper()
		{
			//define type dictionary
			TypeMap = new Dictionary<Type, DbType>();
			TypeMap[typeof(byte)] = DbType.Byte;
			TypeMap[typeof(sbyte)] = DbType.SByte;
			TypeMap[typeof(short)] = DbType.Int16;
			TypeMap[typeof(ushort)] = DbType.UInt16;
			TypeMap[typeof(int)] = DbType.Int32;
			TypeMap[typeof(uint)] = DbType.UInt32;
			TypeMap[typeof(long)] = DbType.Int64;
			TypeMap[typeof(ulong)] = DbType.UInt64;
			TypeMap[typeof(float)] = DbType.Single;
			TypeMap[typeof(double)] = DbType.Double;
			TypeMap[typeof(decimal)] = DbType.Decimal;
			TypeMap[typeof(bool)] = DbType.Boolean;
			TypeMap[typeof(string)] = DbType.String;
			TypeMap[typeof(char)] = DbType.StringFixedLength;
			TypeMap[typeof(Guid)] = DbType.Guid;
			TypeMap[typeof(DateTime)] = DbType.DateTime;
			TypeMap[typeof(DateTimeOffset)] = DbType.DateTimeOffset;
			TypeMap[typeof(TimeSpan)] = DbType.Time;
			TypeMap[typeof(byte[])] = DbType.Binary;
			TypeMap[typeof(byte?)] = DbType.Byte;
			TypeMap[typeof(sbyte?)] = DbType.SByte;
			TypeMap[typeof(short?)] = DbType.Int16;
			TypeMap[typeof(ushort?)] = DbType.UInt16;
			TypeMap[typeof(int?)] = DbType.Int32;
			TypeMap[typeof(uint?)] = DbType.UInt32;
			TypeMap[typeof(long?)] = DbType.Int64;
			TypeMap[typeof(ulong?)] = DbType.UInt64;
			TypeMap[typeof(float?)] = DbType.Single;
			TypeMap[typeof(double?)] = DbType.Double;
			TypeMap[typeof(decimal?)] = DbType.Decimal;
			TypeMap[typeof(bool?)] = DbType.Boolean;
			TypeMap[typeof(char?)] = DbType.StringFixedLength;
			TypeMap[typeof(Guid?)] = DbType.Guid;
			TypeMap[typeof(DateTime?)] = DbType.DateTime;
			TypeMap[typeof(DateTimeOffset?)] = DbType.DateTimeOffset;
			TypeMap[typeof(TimeSpan?)] = DbType.Time;
			TypeMap[typeof(Object)] = DbType.Object;
		}
		#endregion

		#region Public methods

		/// <summary>
		/// The execute sp.
		/// </summary>
		/// <param name="connection">
		/// The connection.
		/// </param>
		/// <param name="spName">
		/// The sp name.
		/// </param>
		/// <param name="dalParams">
		/// The dal params.
		/// </param>
		public static void ExecuteSp(SqlConnection connection, string spName, ref DalParams dalParams)
		{
			var parameters = GetParameters(ref dalParams);

			try
			{
				SqlMapper.Execute(connection, spName, parameters, commandType: CommandType.StoredProcedure);

				AdjustResults(parameters, ref dalParams);
			}
			catch (Exception ex)
			{
				dalParams.TrySetOutputParam("O_ErrCode", (int)DalErrorCodes.SystemError);
				dalParams.TrySetOutputParam("O_ErrMsg",
					string.Format("DAL error : {0}", ex.Message)
					);
			}
			HandleErrors(dalParams, spName);
		}

		/// <summary>
		/// The execute sp.
		/// </summary>
		/// <param name="connection">
		/// The connection.
		/// </param>
		/// <param name="spName">
		/// The sp name.
		/// </param>
		/// <param name="dalParams">
		/// The dal params.
		/// </param>
		/// <typeparam name="T">
		/// </typeparam>
		/// <returns>
		/// The collection of T.
		/// </returns>
		public static IEnumerable<T> ExecuteSp<T>(SqlConnection connection, string spName, ref DalParams dalParams)
		{
			var parameters = GetParameters(ref dalParams);
			IEnumerable<T> result = null;

			try
			{
				var dalResult
					= SqlMapper.Query<T>(connection, sql: spName,
						param: parameters,
						commandType: CommandType.StoredProcedure);

				AdjustResults(parameters, ref dalParams);
				if (dalResult != null)
				{
					result = dalResult.ToList();
				}
			}
			catch (Exception ex)
			{
				dalParams.TrySetOutputParam("O_ErrCode", (int)DalErrorCodes.SystemError);
				dalParams.TrySetOutputParam("O_ErrMsg",
					string.Format("DAL error : {0}", ex.Message)
					);
			}

			HandleErrors(dalParams, spName);
			return result;
		}

		public static void ExecuteSp<T1, T2>(SqlConnection connection, string spName, ref DalParams dalParams, out IEnumerable<T1> recordSet1, out IEnumerable<T2> recordSet2)
		{
			var parameters = GetParameters(ref dalParams);
			recordSet1 = null;
			recordSet2 = null;

			try
			{
				using (var dalResult
					= SqlMapper.QueryMultiple(connection,
											  sql: spName,
											  param: parameters,
											  commandType: CommandType.StoredProcedure))
				{
					recordSet1 = dalResult.Read<T1>().ToList();
					recordSet2 = dalResult.Read<T2>().ToList();
				}

				AdjustResults(parameters, ref dalParams);
			}
			catch (Exception ex)
			{
				dalParams.TrySetOutputParam("O_ErrCode", (int)DalErrorCodes.SystemError);
				dalParams.TrySetOutputParam("O_ErrMsg",
					string.Format("DAL error : {0}", ex.Message)
					);
			}

			HandleErrors(dalParams, spName);
		}

		public static void ExecuteSp<T1, T2, T3>(SqlConnection connection, string spName, ref DalParams dalParams, out IEnumerable<T1> recordSet1, out IEnumerable<T2> recordSet2, out IEnumerable<T3> recordSet3)
		{
			var parameters = GetParameters(ref dalParams);
			recordSet1 = null;
			recordSet2 = null;
			recordSet3 = null;

			try
			{
				using (var dalResult
					= SqlMapper.QueryMultiple(connection,
											  sql: spName,
											  param: parameters,
											  commandType: CommandType.StoredProcedure))
				{
					recordSet1 = dalResult.Read<T1>().ToList();
					recordSet2 = dalResult.Read<T2>().ToList();
					recordSet3 = dalResult.Read<T3>().ToList();
				}

				AdjustResults(parameters, ref dalParams);
			}
			catch (Exception ex)
			{
				dalParams.TrySetOutputParam("O_ErrCode", (int)DalErrorCodes.SystemError);
				dalParams.TrySetOutputParam("O_ErrMsg",
					string.Format("DAL error : {0}", ex.Message)
					);
			}

			HandleErrors(dalParams, spName);
		}

		public static void ExecuteSp<T1, T2, T3, T4>(SqlConnection connection, string spName, ref DalParams dalParams, out IEnumerable<T1> recordSet1, out IEnumerable<T2> recordSet2, out IEnumerable<T3> recordSet3, out IEnumerable<T4> recordSet4)
		{
			var parameters = GetParameters(ref dalParams);
			recordSet1 = null;
			recordSet2 = null;
			recordSet3 = null;
			recordSet4 = null;

			try
			{
				using (var dalResult
					= SqlMapper.QueryMultiple(connection,
											  sql: spName,
											  param: parameters,
											  commandType: CommandType.StoredProcedure))
				{
					recordSet1 = dalResult.Read<T1>().ToList();
					recordSet2 = dalResult.Read<T2>().ToList();
					recordSet3 = dalResult.Read<T3>().ToList();
					recordSet4 = dalResult.Read<T4>().ToList();
				}

				AdjustResults(parameters, ref dalParams);
			}
			catch (Exception ex)
			{
				dalParams.TrySetOutputParam("O_ErrCode", (int)DalErrorCodes.SystemError);
				dalParams.TrySetOutputParam("O_ErrMsg",
					string.Format("DAL error : {0}", ex.Message)
					);
			}

			HandleErrors(dalParams, spName);
		}

		public static void ExecuteSp<T1, T2, T3, T4, T5, T6>(SqlConnection connection, string spName, ref DalParams dalParams, out IEnumerable<T1> recordSet1, out IEnumerable<T2> recordSet2, out IEnumerable<T3> recordSet3, out IEnumerable<T4> recordSet4, out IEnumerable<T5> recordSet5, out IEnumerable<T6> recordSet6)
		{
			var parameters = GetParameters(ref dalParams);
			recordSet1 = null;
			recordSet2 = null;
			recordSet3 = null;
			recordSet4 = null;
			recordSet5 = null;
			recordSet6 = null;

			try
			{
				using (var dalResult
					= SqlMapper.QueryMultiple(connection,
											  sql: spName,
											  param: parameters,
											  commandType: CommandType.StoredProcedure))
				{
					recordSet1 = dalResult.Read<T1>().ToList();
					recordSet2 = dalResult.Read<T2>().ToList();
					recordSet3 = dalResult.Read<T3>().ToList();
					recordSet4 = dalResult.Read<T4>().ToList();
					recordSet5 = dalResult.Read<T5>().ToList();
					recordSet6 = dalResult.Read<T6>().ToList();
				}

				AdjustResults(parameters, ref dalParams);
			}
			catch (Exception ex)
			{
				dalParams.TrySetOutputParam("O_ErrCode", (int)DalErrorCodes.SystemError);
				dalParams.TrySetOutputParam("O_ErrMsg",
					string.Format("DAL error : {0}", ex.Message)
					);
			}

			HandleErrors(dalParams, spName);
		}
		#endregion

		#region Private methods and helpers

		/// <summary>
		/// The get parameters.
		/// </summary>
		/// <param name="dalParams">
		/// The dal params.
		/// </param>
		/// <returns>
		/// The <see cref="DynamicParameters"/>.
		/// </returns>
		private static DynamicParameters GetParameters(ref DalParams dalParams)
		{
			var parameters = new DynamicParameters();
			//Input parameters
			foreach (var inParam in (IDictionary<string, object>)dalParams.Input)
			{
				parameters.Add(string.Format("@{0}", inParam.Key), inParam.Value);
			}

			//output parameters
			foreach (var outParam in (IDictionary<string, object>)dalParams.Output)
			{
				if (dalParams.OutSize.ContainsKey(outParam.Key))
				{
					parameters.Add(string.Format("@{0}", outParam.Key),
								   direction: ParameterDirection.Output,
								   size: dalParams.OutSize[outParam.Key],
								   dbType: TypeMap[outParam.Value.GetType()]
						);
				}
				else if (outParam.Value is decimal)
				{
					parameters.Add(string.Format("@{0}", outParam.Key),
								   direction: ParameterDirection.Output,
								   size: 40,
								   dbType: DbType.String
						);
				}
				else
				{
					parameters.Add(string.Format("@{0}", outParam.Key),
								   direction: ParameterDirection.Output,
								   dbType: TypeMap[outParam.Value.GetType()]);
				}
			}

			//return value
			if (dalParams.ReturnValue is IDictionary<string, object>)
			{
				var retValue = (dalParams.ReturnValue as IDictionary<string, object>).FirstOrDefault();

				if (retValue.Key != null)
					parameters.Add(name: string.Format("@{0}", retValue.Key),
								   direction: ParameterDirection.ReturnValue,
								   dbType: TypeMap[retValue.Value.GetType()]);
			}
			return parameters;
		}

		/// <summary>
		/// The adjust results.
		/// </summary>
		/// <param name="parameters">
		/// The parameters.
		/// </param>
		/// <param name="dalParams">
		/// The dal params.
		/// </param>
		private static void AdjustResults(DynamicParameters parameters, ref DalParams dalParams)
		{
			var result = ((IDictionary<string, dynamic>)dalParams.Output)
				.ToDictionary(outParam => outParam.Key, outParam => outParam.Value);
			foreach (var o in result)
			{
				if (o.Value is Decimal)
				{
					decimal decResult;
					var t = parameters.Get<string>(string.Format("@{0}", o.Key));
					if (Decimal.TryParse(t, NumberStyles.AllowDecimalPoint, CultureInfo.InvariantCulture, out decResult))
					{
						dalParams.TrySetOutputParam(o.Key, decResult);
					}
				}
				else
				{
					dalParams.TrySetOutputParam(o.Key, parameters.Get<dynamic>(string.Format("@{0}", o.Key)));
				}
			}

			var returns = ((IDictionary<string, dynamic>)dalParams.ReturnValue)
				.ToDictionary(retParam => retParam.Key, retParam => retParam.Value);
			foreach (var r in returns)
			{
				dalParams.TrySetOutputParam(r.Key, parameters.Get<dynamic>(string.Format("@{0}", r.Key)));
			}
		}

		private static void HandleErrors(DalParams parameters, string spName)
		{
			//Output parameters
			var errorCode = parameters.Output.O_ErrCode;

			// If there is an error
			if (errorCode != (int)DalErrorCodes.Ok)
			{
				var sb = new StringBuilder();

				if (parameters.Input is ExpandoObject)
				{
					foreach (var input in ((ExpandoObject)parameters.Input))
					{
						sb.AppendFormat("{0}: {1}", input.Key, input.Value);
					}
				}

				throw new Exception(
					string.Format("{0} {1} SP Name: {3} {1} Input: {2}", parameters.Output.O_ErrMsg, Environment.NewLine, sb, spName));
			}
		}

		#endregion
	}
}
