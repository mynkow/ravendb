using System;
using System.Collections;
using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using Voron;
using Voron.Impl;

namespace Regression.PageLocator
{
    public struct PageHandlePtrV3
    {        
        public readonly long PageNumber;        
        public readonly Page Value;
        public readonly bool IsWritable;

        private const int Invalid = -1;

        public PageHandlePtrV3(long pageNumber, Page value, bool isWritable)
        {
            this.Value = value;
            this.PageNumber = pageNumber;
            this.IsWritable = isWritable;
        }
    }

    public class PageLocatorV3
    {
        private static readonly Vector<ushort> _indexes;


        private readonly LowLevelTransaction _tx;
        // This is the size of the cache, required to be _cacheSize % Vector<long>.Count == 0
        private readonly int _cacheSize;
        
        private readonly ushort[] _fingerprints;
        private readonly PageHandlePtrV3[] _cache;

        private int _current;

        static PageLocatorV3()
        {
            var indexes = new ushort[Vector<ushort>.Count];
            for (ushort i = 0; i < Vector<ushort>.Count; i++)
                indexes[i] = (ushort)(i + 1);

            _indexes = new Vector<ushort>(indexes);
        }

        public PageLocatorV3(LowLevelTransaction tx, int cacheSize = 4)
        {
            //Debug.Assert(tx != null);
            _tx = tx;

            if (tx != null)
                Debug.Fail("");

            // Align cache size to Vector<ushort>.Count
            _cacheSize = cacheSize;
            if (_cacheSize % Vector<ushort>.Count != 0)
                _cacheSize += Vector<ushort>.Count - cacheSize % Vector<ushort>.Count;

            _current = 0;

            _cache = new PageHandlePtrV3[_cacheSize];

            _fingerprints = new ushort[_cacheSize];
            for (ushort i = 0; i < _fingerprints.Length; i++)
            {
                _fingerprints[i] = ushort.MaxValue;
            }
        }

        public Page GetReadOnlyPage(long pageNumber)
        {
            ushort fingerprint = (ushort)(pageNumber % (ushort.MaxValue - 1));

            var lookup = new Vector<ushort>(fingerprint);
            for (int i = 0; i < _cacheSize; i += Vector<ushort>.Count)
            {
                var pageNumbers = new Vector<ushort>(_fingerprints, i);
                var comparison = Vector.Equals(pageNumbers, lookup);
                var result = Vector.ConditionalSelect(comparison, Vector<ushort>.One, Vector<ushort>.Zero);
                ushort index = Vector.Dot(_indexes, result);

                if (index != 0)
                {
                    int j = i + index - 1;
                    if (_cache[j].PageNumber == pageNumber)
                    {
                        return _cache[j].Value;
                    }

                    _cache[j] = new PageHandlePtrV3(pageNumber, LowLevelTransactionStub.GetPage(pageNumber), false);
                    return _cache[j].Value;
                }                
            }

            // If we got here, there was a cache miss
            _current = (_current++) % _cacheSize;
            _cache[_current] = new PageHandlePtrV3(pageNumber, LowLevelTransactionStub.GetPage(pageNumber), false);
            _fingerprints[_current] = fingerprint;

            return _cache[_current].Value;
        }

        public Page GetWritablePage(long pageNumber)
        {
            ushort fingerprint = (ushort) (pageNumber% (ushort.MaxValue - 1));

            var lookup = new Vector<ushort>(fingerprint);
            for (int i = 0; i < _cacheSize; i += Vector<ushort>.Count)
            {
                var pageNumbers = new Vector<ushort>(_fingerprints, i);
                var comparison = Vector.Equals(pageNumbers, lookup);
                var result = Vector.ConditionalSelect(comparison, Vector<ushort>.One, Vector<ushort>.Zero);
                ushort index = Vector.Dot(_indexes, result);

                if (index != 0)
                {
                    int j = i + index - 1;
                    if (_cache[j].PageNumber == pageNumber && _cache[j].IsWritable)
                    {
                        return _cache[j].Value;
                    }

                    _cache[j] = new PageHandlePtrV3(pageNumber, LowLevelTransactionStub.GetPage(pageNumber), true);
                    return _cache[j].Value;
                }
            }

            // If we got here, there was a cache miss
            _current = (_current++) % _cacheSize;
            _cache[_current] = new PageHandlePtrV3(pageNumber, LowLevelTransactionStub.GetPage(pageNumber), true);
            _fingerprints[_current] = fingerprint;

            return _cache[_current].Value;
        }

        public void Clear()
        {
            _current = 0;
            Array.Clear(_cache, 0, _cache.Length);
            Array.Clear(_fingerprints, 0, _cache.Length);
            for (int i = 0; i < _fingerprints.Length; i++)
                _fingerprints[i] = ushort.MaxValue;
        }

        public void Reset(long pageNumber)
        {
            ushort fingerprint = (ushort)(pageNumber % (ushort.MaxValue - 1));

            var lookup = new Vector<ushort>(fingerprint);
            for (int i = 0; i < _cacheSize; i += Vector<ushort>.Count)
            {
                var pageNumbers = new Vector<ushort>(_fingerprints, i);
                var comparison = Vector.Equals(pageNumbers, lookup);
                var result = Vector.ConditionalSelect(comparison, Vector<ushort>.One, Vector<ushort>.Zero);
                ushort index = Vector.Dot(_indexes, result);
                if (index != 0)
                {
                    int j = i + index - 1;
                    _cache[j] = new PageHandlePtrV3();
                    _fingerprints[j] = ushort.MaxValue;

                    return;
                }
            }
        }
    }
}