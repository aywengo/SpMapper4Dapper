using System.Collections.Generic;
using System.Dynamic;

namespace SpMapper4Dapper
{
	public class DalParams
	{
		public dynamic Input { get; set; }
		public dynamic Output { get; set; }
		public dynamic ReturnValue { get; set; }

		public Dictionary<string, int> OutSize { get; set; }

		public DalParams()
		{
			this.Input = new ExpandoObject();
			this.Output = new ExpandoObject();
			this.OutSize = new Dictionary<string, int>();
			this.ReturnValue = new ExpandoObject();

			// define default output
			// TODO : use strings consts
			this.Output.O_ErrCode = 0;
			this.OutSize.Add("O_ErrCode", 2);
			this.Output.O_ErrMsg = string.Empty;
			this.OutSize.Add("O_ErrMsg", 2000);
		}

		public void TrySetOutputParam(string name, dynamic value)
		{
			var outParams = this.Output as IDictionary<string, object>;
			if (outParams != null)
			{
				outParams[name] = value;
			}
		}
	}
}