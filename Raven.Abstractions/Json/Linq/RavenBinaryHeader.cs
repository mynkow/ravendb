using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Raven.Json.Linq
{
    public class RavenBinaryHeader
    {
        private readonly string[] properties;
        public RavenBinaryHeader( string[] properties )
        {
            this.properties = properties;
        }

        public bool TryGetPropertyName ( int index, out string value )
        {            
            if(  index >= properties.Length || index < 0)            
            {
                value = null;
                return false;
            }                    

            value = properties[index];
            return !string.IsNullOrWhiteSpace(value);
        }
    }
}
