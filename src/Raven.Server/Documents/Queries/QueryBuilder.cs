﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using Lucene.Net.Analysis;
using Lucene.Net.QueryParsers;
using Lucene.Net.Search;
using Raven.Client.Documents.Queries;
using Raven.Server.Documents.Queries.Parse;
using Raven.Server.Utils;
using Raven.Server.Documents.Queries.Parser;
using Raven.Server.Documents.Indexes;
using Sparrow.Json;
using Query = Raven.Server.Documents.Queries.Parser.Query;

namespace Raven.Server.Documents.Queries
{
    public static class QueryBuilder
    {
        private static readonly TermLuceneASTNode WildCardTerm = new TermLuceneASTNode() { Term = "*", Type = TermLuceneASTNode.TermType.WildCardTerm };
        private static readonly TermLuceneASTNode NullTerm = new TermLuceneASTNode() { Term = "NULL", Type = TermLuceneASTNode.TermType.Null };

        public static bool UseLuceneASTParser { get; set; } = true;

        public static Lucene.Net.Search.Query BuildQuery(string query, Analyzer analyzer)
        {
            throw new NotSupportedException("TODO arek - remove me");
        }

        public static Lucene.Net.Search.Query BuildQuery(QueryMetadata metadata, BlittableJsonReaderObject parameters, Analyzer analyzer)
        {
            using (CultureHelper.EnsureInvariantCulture())
            {
                var node = ToLuceneNode(metadata.Query, metadata.Query.Where, metadata, parameters);

                var luceneQuery = node.ToQuery(new LuceneASTQueryConfiguration
                {
                    Analayzer = analyzer,
                    DefaultOperator = QueryOperator.And,
                    FieldName = new FieldName(string.Empty)
                });

                // The parser already throws parse exception if there is a syntax error.
                // We now return null in the case of a term query that has been fully analyzed, so we need to return a valid query.
                return luceneQuery ?? new BooleanQuery();
            }
        }

        private static LuceneASTNodeBase ToLuceneNode(Parser.Query query, QueryExpression expression, QueryMetadata metadata, BlittableJsonReaderObject parameters, string boost = null)
        {
            if (expression == null)
                return new AllDocumentsLuceneASTNode();

            if (expression.Type == OperatorType.Field)
            {
                throw new NotImplementedException("OperatorType.Field");

                //                return new FieldLuceneASTNode()
                //                {
                //                    FieldName = new FieldName(QueryExpression.Extract(query, expression.Field)),
                //                    Node = new TermLuceneASTNode()
                //                    {
                //                        Term = QueryExpression.Extract(query, expression.Value ?? expression.First),
                //                    }
                //                };
            }

            switch (expression.Type)
            {
                case OperatorType.Equal:
                case OperatorType.GreaterThen:
                case OperatorType.LessThen:
                case OperatorType.LessThenEqual:
                case OperatorType.GreaterThenEqual:
                    {
                        var fieldName = QueryExpression.Extract(query.QueryText, expression.Field);
                        var (value, valueType) = GetValue(fieldName, query, metadata, parameters, expression.Value);

                        var (luceneFieldName, fieldType) = GetLuceneField(fieldName, valueType);

                        if (expression.Type == OperatorType.Equal && fieldType == FieldName.FieldType.String)
                        {
                            return CreateFieldNode(luceneFieldName, fieldType, CreateTermNode(value, valueType));
                        }

                        RangeLuceneASTNode rangeNode = new RangeLuceneASTNode()
                        {
                            InclusiveMin = false,
                            InclusiveMax = false,
                            RangeMin = WildCardTerm,
                            RangeMax = NullTerm
                        };

                        switch (expression.Type)
                        {
                            case OperatorType.Equal:
                                rangeNode.InclusiveMin = true;
                                rangeNode.InclusiveMax = true;
                                var node = CreateTermNode(value, valueType);
                                rangeNode.RangeMin = node;
                                rangeNode.RangeMax = node;
                                break;
                            case OperatorType.LessThen:
                                rangeNode.RangeMax = CreateTermNode(value, valueType);
                                break;
                            case OperatorType.GreaterThen:
                                rangeNode.RangeMin = CreateTermNode(value, valueType);
                                break;
                            case OperatorType.LessThenEqual:
                                rangeNode.InclusiveMax = true;
                                rangeNode.RangeMax = CreateTermNode(value, valueType);
                                break;
                            case OperatorType.GreaterThenEqual:
                                rangeNode.InclusiveMin = true;
                                rangeNode.RangeMin = CreateTermNode(value, valueType);
                                break;
                        }

                        return CreateFieldNode(luceneFieldName, fieldType, rangeNode);
                    }
                case OperatorType.Between:
                    {
                        var fieldName = QueryExpression.Extract(query.QueryText, expression.Field);
                        var (valueFirst, valueFirstType) = GetValue(fieldName, query, metadata, parameters, expression.First);
                        var (valueSecond, valueSecondType) = GetValue(fieldName, query, metadata, parameters, expression.Second);

                        var (luceneFieldName, fieldType) = GetLuceneField(fieldName, valueFirstType);

                        return CreateFieldNode(luceneFieldName, fieldType, new RangeLuceneASTNode
                        {
                            InclusiveMin = true,
                            InclusiveMax = true,
                            RangeMin = CreateTermNode(valueFirst, valueFirstType),
                            RangeMax = CreateTermNode(valueSecond, valueSecondType)
                        });
                    }
                case OperatorType.In:
                    {
                        var fieldName = QueryExpression.Extract(query.QueryText, expression.Field);

                        var matches = new List<TermLuceneASTNode>(expression.Values.Count);
                        foreach (var valueToken in expression.Values)
                        {
                            foreach (var (value, valueType) in GetValues(fieldName, query, metadata, parameters, valueToken))
                                matches.Add(CreateTermNode(value, valueType));
                        }

                        var luceneFieldName = GetLuceneField(fieldName, metadata.Fields[fieldName]).LuceneFieldName;

                        return new MethodLuceneASTNode($"@in<{luceneFieldName}>", matches)
                        {
                            FieldName = luceneFieldName,
                            MethodName = "in"
                        };
                    }
                case OperatorType.And:
                    return new OperatorLuceneASTNode(ToLuceneNode(query, expression.Left, metadata, parameters), ToLuceneNode(query, expression.Right, metadata, parameters), OperatorLuceneASTNode.Operator.AND,
                        true);
                case OperatorType.Or:
                    return new OperatorLuceneASTNode(ToLuceneNode(query, expression.Left, metadata, parameters), ToLuceneNode(query, expression.Right, metadata, parameters), OperatorLuceneASTNode.Operator.OR,
                        true);
                case OperatorType.AndNot:
                case OperatorType.OrNot:
                case OperatorType.Method:
                    var methodName = QueryExpression.Extract(query.QueryText, expression.Field);
                    var methodType = GetMethodType(methodName);

                    switch (methodType)
                    {
                        case MethodType.Search:
                            return HandleSearch(query, expression, metadata, parameters, boost);
                        case MethodType.Boost:
                            return HandleBoost(query, expression, metadata, parameters);
                        case MethodType.StartsWith:
                            return HandleStartsWith(query, expression, metadata, parameters, boost);
                        case MethodType.EndsWith:
                            return HandleEndsWith(query, expression, metadata, parameters, boost);
                        default:
                            throw new NotSupportedException($"Method '{methodType}' is not supported.");
                    }

                    throw new NotImplementedException("Type: " + expression.Type);
                //writer.WritePropertyName("Method");
                //WriteValue(query, writer, Field.TokenStart, Field.TokenLength, Field.EscapeChars);
                //writer.WritePropertyName("Arguments");
                //writer.WriteStartArray();
                //foreach (var arg in Arguments)
                //{
                //    if (arg is QueryExpression qe)
                //    {
                //        qe.ToJsonAst(query, writer);
                //    }
                //    else if (arg is FieldToken field)
                //    {
                //        writer.WriteStartObject();
                //        writer.WritePropertyName("Field");
                //        WriteValue(query, writer, field.TokenStart, field.TokenLength, field.EscapeChars);
                //        writer.WriteEndObject();
                //    }
                //    else
                //    {
                //        var val = (ValueToken)arg;
                //        WriteValue(query, writer, val.TokenStart, val.TokenLength, val.EscapeChars,
                //            val.Type == ValueTokenType.Double || val.Type == ValueTokenType.Long);
                //    }
                //}
                //writer.WriteEndArray();
                //break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        private static LuceneASTNodeBase HandleStartsWith(Query query, QueryExpression expression, QueryMetadata metadata, BlittableJsonReaderObject parameters, string boost)
        {
            var fieldName = QueryExpression.Extract(query.QueryText, (FieldToken)expression.Arguments[0]);
            var (value, valueType) = GetValue(fieldName, query, metadata, parameters, (ValueToken)expression.Arguments[1]);

            if (string.IsNullOrEmpty(value))
                value = "*";
            else
                value += "*";

            return CreateFieldNode(fieldName, FieldName.FieldType.String, new TermLuceneASTNode
            {
                Term = value,
                Type = TermLuceneASTNode.TermType.PrefixTerm
            });
        }

        private static LuceneASTNodeBase HandleEndsWith(Query query, QueryExpression expression, QueryMetadata metadata, BlittableJsonReaderObject parameters, string boost)
        {
            var fieldName = QueryExpression.Extract(query.QueryText, (FieldToken)expression.Arguments[0]);
            var (value, valueType) = GetValue(fieldName, query, metadata, parameters, (ValueToken)expression.Arguments[1]);

            value = string.IsNullOrEmpty(value) 
                ? "*" 
                : value.Insert(0, "*");

            return CreateFieldNode(fieldName, FieldName.FieldType.String, new TermLuceneASTNode
            {
                Term = value,
                Type = TermLuceneASTNode.TermType.WildCardTerm
            });
        }

        private static LuceneASTNodeBase HandleBoost(Query query, QueryExpression expression, QueryMetadata metadata, BlittableJsonReaderObject parameters)
        {
            var boost = QueryExpression.Extract(query.QueryText, (ValueToken)expression.Arguments[1]);
            expression = (QueryExpression)expression.Arguments[0];

            return ToLuceneNode(query, expression, metadata, parameters, boost);
        }

        private static LuceneASTNodeBase HandleSearch(Query query, QueryExpression expression, QueryMetadata metadata, BlittableJsonReaderObject parameters, string boost)
        {
            var fieldName = QueryExpression.Extract(query.QueryText, (FieldToken)expression.Arguments[0]);
            var (value, valueType) = GetValue(fieldName, query, metadata, parameters, (ValueToken)expression.Arguments[1]);

            LuceneASTNodeBase node;
            var values = value.Split(' ');

            if (values.Length == 1)
                node = CreateNode(CreateTermNode(values[0], valueType), null, boost);
            else
            {
                LuceneASTNodeBase left = null;
                for (var i = values.Length - 2; i >= 0; i--)
                {
                    var v = values[i];

                    if (left == null)
                    {
                        left = CreateTermNode(v, valueType);
                        continue;
                    }

                    left = CreateNode(CreateTermNode(v, valueType), left, null);
                }

                node = CreateNode(left, CreateTermNode(values[values.Length - 1], valueType), boost);
            }

            return CreateFieldNode(fieldName, FieldName.FieldType.String, node);

            LuceneASTNodeBase CreateNode(LuceneASTNodeBase left, LuceneASTNodeBase right, string boostValue)
            {
                var result = right == null
                    ? left
                    : new OperatorLuceneASTNode(left, right, OperatorLuceneASTNode.Operator.OR, isDefaultOperatorAnd: false);

                if (boostValue == null)
                    return result;

                return new ParenthesisLuceneASTNode
                {
                    Boost = boost,
                    Node = result
                };
            }
        }

        public static IEnumerable<(string Value, ValueTokenType Type)> GetValues(string fieldName, Parser.Query query, QueryMetadata metadata, BlittableJsonReaderObject parameters, ValueToken value)
        {
            var valueOrParameterName = QueryExpression.Extract(query.QueryText, value);

            if (value.Type == ValueTokenType.Parameter)
            {
                var expectedValueType = metadata.Fields[fieldName];

                if (parameters == null)
                    throw new InvalidOperationException();

                if (parameters.TryGetMember(valueOrParameterName, out var parameterValue) == false)
                    throw new InvalidOperationException();

                var array = parameterValue as BlittableJsonReaderArray;
                if (array != null)
                {
                    foreach (var item in UnwrapArray(array))
                    {
                        if (expectedValueType != item.Type)
                            throw new InvalidOperationException();

                        yield return item;
                    }

                    yield break;
                }

                var parameterValueType = GetValueTokenType(parameterValue);
                if (expectedValueType != parameterValueType)
                    throw new InvalidOperationException();

                yield return (parameterValue.ToString(), parameterValueType);
            }

            yield return (valueOrParameterName, value.Type);
        }

        public static (string Value, ValueTokenType Type) GetValue(string fieldName, Parser.Query query, QueryMetadata metadata, BlittableJsonReaderObject parameters, ValueToken value)
        {
            var valueOrParameterName = QueryExpression.Extract(query.QueryText, value);

            if (value.Type == ValueTokenType.Parameter)
            {
                var expectedValueType = metadata.Fields[fieldName];

                if (parameters == null)
                    throw new InvalidOperationException();

                if (parameters.TryGetMember(valueOrParameterName, out var parameterValue) == false)
                    throw new InvalidOperationException();

                var parameterValueType = GetValueTokenType(parameterValue);

                if (expectedValueType != parameterValueType)
                    throw new InvalidOperationException();

                return (parameterValue.ToString(), parameterValueType); // TODO [ppekrol] avoid ToString()
            }

            return (valueOrParameterName, value.Type);
        }

        private static (string LuceneFieldName, FieldName.FieldType LuceneFieldType) GetLuceneField(string fieldName, ValueTokenType valueType)
        {
            fieldName = IndexField.ReplaceInvalidCharactersInFieldName(fieldName);

            switch (valueType)
            {
                case ValueTokenType.String:
                    return (fieldName, FieldName.FieldType.String);
                case ValueTokenType.Double:
                    return (fieldName + Client.Constants.Documents.Indexing.Fields.RangeFieldSuffixDouble, FieldName.FieldType.Double); // TODO arek - avoid +
                case ValueTokenType.Long:
                    return (fieldName + Client.Constants.Documents.Indexing.Fields.RangeFieldSuffixLong, FieldName.FieldType.Long); // TODO arek - avoid +
                default:
                    ThrowUnhandledValueTokenType(valueType);
                    break;
            }

            Debug.Assert(false);

            return (null, FieldName.FieldType.String);
        }

        private static IEnumerable<(string Value, ValueTokenType Type)> UnwrapArray(BlittableJsonReaderArray array)
        {
            foreach (var item in array)
            {
                var innerArray = item as BlittableJsonReaderArray;
                if (innerArray != null)
                {
                    foreach (var innerItem in UnwrapArray(innerArray))
                        yield return innerItem;

                    continue;
                }

                yield return (item.ToString(), GetValueTokenType(item));
            }
        }

        private static FieldLuceneASTNode CreateFieldNode(string fieldName, FieldName.FieldType fieldType, LuceneASTNodeBase node)
        {
            return new FieldLuceneASTNode
            {
                FieldName = new FieldName(fieldName, fieldType),
                Node = node
            };
        }

        private static TermLuceneASTNode CreateTermNode(string value, ValueTokenType valueType)
        {
            switch (valueType)
            {
                case ValueTokenType.Null:
                    return new TermLuceneASTNode
                    {
                        Type = TermLuceneASTNode.TermType.Null
                    };
                case ValueTokenType.False:
                case ValueTokenType.True:
                    throw new NotImplementedException("expression.Value.Type:" + valueType);
                default:
                    TermLuceneASTNode.TermType type;

                    switch (valueType)
                    {
                        case ValueTokenType.String:
                            type = TermLuceneASTNode.TermType.Quoted;
                            break;
                        case ValueTokenType.Long:
                            type = TermLuceneASTNode.TermType.Long;
                            break;
                        case ValueTokenType.Double:
                            type = TermLuceneASTNode.TermType.Double;
                            break;
                        default:
                            throw new NotImplementedException("Unhandled value type: " + valueType);
                            //type = TermLuceneASTNode.TermType.Quoted;
                            //break;
                    }

                    return new TermLuceneASTNode
                    {
                        Term = value,
                        Type = type
                    };
            }
        }

        private static Lucene.Net.Search.Query Lucene(string query, QueryOperator defaultOperator, string defaultField, Analyzer analyzer)
        {
            using (CultureHelper.EnsureInvariantCulture())
            {
                try
                {
                    var parser = new LuceneQueryParser();
                    parser.IsDefaultOperatorAnd = defaultOperator == QueryOperator.And;
                    parser.Parse(query);

                    var res = parser.LuceneAST.ToQuery(new LuceneASTQueryConfiguration
                    {
                        Analayzer = analyzer,
                        DefaultOperator = QueryOperator.And,
                        FieldName = new FieldName(defaultField ?? string.Empty)
                    });
                    // The parser already throws parse exception if there is a syntax error.
                    // We now return null in the case of a term query that has been fully analyzed, so we need to return a valid query.
                    return res ?? new BooleanQuery();
                }
                catch (ParseException pe)
                {
                    throw new ParseException("Could not parse: '" + query + "'", pe);
                }
            }
        }

        public static string Unescape(string term)
        {
            // method doesn't allocate a StringBuilder unless the string requires unescaping
            // also this copies chunks of the original string into the StringBuilder which
            // is far more efficient than copying character by character because StringBuilder
            // can access the underlying string data directly

            if (string.IsNullOrEmpty(term))
            {
                return term;
            }

            bool isPhrase = term.StartsWith("\"") && term.EndsWith("\"");
            int start = 0;
            int length = term.Length;
            StringBuilder buffer = null;
            char prev = '\0';
            for (int i = start; i < length; i++)
            {
                char ch = term[i];
                if (prev != '\\')
                {
                    prev = ch;
                    continue;
                }
                prev = '\0'; // reset
                switch (ch)
                {
                    case '*':
                    case '?':
                    case '+':
                    case '-':
                    case '&':
                    case '|':
                    case '!':
                    case '(':
                    case ')':
                    case '{':
                    case '}':
                    case '[':
                    case ']':
                    case '^':
                    case '"':
                    case '~':
                    case ':':
                    case '\\':
                        {
                            if (buffer == null)
                            {
                                // allocate builder with headroom
                                buffer = new StringBuilder(length * 2);
                            }
                            // append any leading substring
                            buffer.Append(term, start, i - start - 1);
                            buffer.Append(ch);
                            start = i + 1;
                            break;
                        }
                }
            }

            if (buffer == null)
            {
                if (isPhrase)
                    return term.Substring(1, term.Length - 2);
                // no changes required
                return term;
            }

            if (length > start)
            {
                // append any trailing substring
                buffer.Append(term, start, length - start);
            }

            return buffer.ToString();
        }

        public static ValueTokenType GetValueTokenType(object parameterValue, bool unwrapArrays = false)
        {
            if (parameterValue == null)
                return ValueTokenType.String;

            if (parameterValue is LazyStringValue || parameterValue is LazyCompressedStringValue)
                return ValueTokenType.String;

            if (parameterValue is LazyNumberValue)
                return ValueTokenType.Double;

            if (parameterValue is long)
                return ValueTokenType.Long;

            if (parameterValue is bool)
                return (bool)parameterValue ? ValueTokenType.True : ValueTokenType.False;

            if (unwrapArrays)
            {
                var array = parameterValue as BlittableJsonReaderArray;
                if (array != null)
                {
                    if (array.Length == 0) // TODO [ppekrol]
                        throw new InvalidOperationException();

                    return GetValueTokenType(array[0], unwrapArrays: true);
                }
            }

            throw new NotImplementedException();
        }

        private static MethodType GetMethodType(string methodName)
        {
            if (string.Equals(methodName, "search", StringComparison.OrdinalIgnoreCase))
                return MethodType.Search;

            if (string.Equals(methodName, "boost", StringComparison.OrdinalIgnoreCase))
                return MethodType.Boost;

            if (string.Equals(methodName, "startsWith", StringComparison.OrdinalIgnoreCase))
                return MethodType.StartsWith;

            if (string.Equals(methodName, "endsWith", StringComparison.OrdinalIgnoreCase))
                return MethodType.EndsWith;

            throw new NotSupportedException($"Method '{methodName}' is not supported.");
        }

        private static void ThrowUnhandledValueTokenType(ValueTokenType type)
        {
            throw new NotSupportedException($"Unhandled toke type: {type}");
        }

        private enum MethodType
        {
            Search,
            Boost,
            StartsWith,
            EndsWith
        }
    }
}