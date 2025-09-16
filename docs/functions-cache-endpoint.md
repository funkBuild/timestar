# GET /functions/cache - Get Function Cache Statistics

## Overview
The function cache endpoint provides detailed statistics about the function result caching system, including cache hit rates, memory usage, eviction patterns, and cache effectiveness metrics. This data is crucial for optimizing cache configuration and understanding query performance patterns.

## Endpoint Details
- **URL**: `/functions/cache`
- **Method**: `GET`
- **Content-Type**: `application/json`
- **Authentication**: None required

## Request Format

### Basic Request
```bash
curl -X GET http://localhost:8086/functions/cache
```

### Query Parameters
- **function**: Filter cache statistics for a specific function name
- **metric**: Specific cache metric (hit_rate, memory_usage, evictions, etc.)
- **timeframe**: Time period for statistics (1h, 24h, 7d) (default: 24h)
- **details**: Include detailed cache entry information (true/false) (default: false)

### Request Examples
```bash
# Get overall cache statistics
curl "http://localhost:8086/functions/cache"

# Get cache statistics for specific function
curl "http://localhost:8086/functions/cache?function=sma"

# Get detailed cache information
curl "http://localhost:8086/functions/cache?details=true"

# Get cache hit rate metrics only
curl "http://localhost:8086/functions/cache?metric=hit_rate"

# Get cache statistics over last 7 days
curl "http://localhost:8086/functions/cache?timeframe=7d"
```

## Response Format

### Complete Cache Statistics
```json
{
  "status": "success",
  "cache_overview": {
    "total_cache_size_mb": 128.0,
    "used_cache_size_mb": 89.7,
    "cache_utilization_percent": 70.1,
    "total_cache_entries": 2847,
    "cache_efficiency_score": 8.3
  },
  "performance_metrics": {
    "overall_hit_rate": 0.847,
    "overall_miss_rate": 0.153,
    "hits_24h": 45230,
    "misses_24h": 8164,
    "total_requests_24h": 53394,
    "cache_save_time_ms": 234567.8,
    "average_lookup_time_ms": 0.12
  },
  "memory_statistics": {
    "allocated_memory_mb": 128.0,
    "used_memory_mb": 89.7,
    "available_memory_mb": 38.3,
    "fragmentation_percent": 5.2,
    "largest_entry_mb": 12.4,
    "average_entry_size_kb": 32.5
  },
  "eviction_statistics": {
    "total_evictions_24h": 156,
    "eviction_rate_per_hour": 6.5,
    "eviction_policy": "lru",
    "evictions_by_reason": {
      "memory_pressure": 89,
      "ttl_expired": 45,
      "manual_invalidation": 22
    },
    "average_entry_lifetime_minutes": 87.3
  },
  "function_breakdown": [
    {
      "function": "sma",
      "cache_entries": 1024,
      "hit_rate": 0.892,
      "memory_usage_mb": 23.5,
      "evictions_24h": 12,
      "avg_entry_size_kb": 28.9,
      "cache_effectiveness": "high"
    },
    {
      "function": "derivative",
      "cache_entries": 567,
      "hit_rate": 0.734,
      "memory_usage_mb": 18.7,
      "evictions_24h": 34,
      "avg_entry_size_kb": 35.2,
      "cache_effectiveness": "medium"
    },
    {
      "function": "scale",
      "cache_entries": 892,
      "hit_rate": 0.951,
      "memory_usage_mb": 15.2,
      "evictions_24h": 3,
      "avg_entry_size_kb": 18.4,
      "cache_effectiveness": "very_high"
    }
  ],
  "optimization_recommendations": [
    "Consider increasing cache size to reduce evictions for derivative function",
    "SMA function shows excellent cache performance - no changes needed",
    "Scale function cache is highly effective due to small entry sizes"
  ],
  "trends": {
    "hit_rate_trend": "stable",
    "memory_usage_trend": "increasing",
    "eviction_rate_trend": "stable"
  }
}
```

### Single Function Cache Statistics
```json
{
  "status": "success",
  "function": "sma",
  "cache_statistics": {
    "basic_metrics": {
      "total_entries": 1024,
      "hit_rate": 0.892,
      "miss_rate": 0.108,
      "hits_24h": 8234,
      "misses_24h": 995,
      "total_requests_24h": 9229
    },
    "memory_metrics": {
      "total_memory_mb": 23.5,
      "average_entry_size_kb": 28.9,
      "largest_entry_kb": 156.7,
      "smallest_entry_kb": 2.1,
      "memory_efficiency_score": 8.7
    },
    "temporal_metrics": {
      "average_entry_age_minutes": 45.2,
      "oldest_entry_age_hours": 18.7,
      "newest_entry_age_seconds": 12,
      "entry_turnover_rate": 0.23
    },
    "eviction_details": {
      "evictions_24h": 12,
      "eviction_reasons": {
        "memory_pressure": 8,
        "ttl_expired": 3,
        "manual_invalidation": 1
      },
      "average_evicted_entry_age_minutes": 78.4,
      "eviction_impact_on_performance": "low"
    },
    "parameter_analysis": {
      "cached_parameter_combinations": {
        "window=5": 245,
        "window=10": 198,
        "window=20": 156,
        "window=50": 89,
        "other": 336
      },
      "hit_rate_by_parameter": {
        "window=5": 0.923,
        "window=10": 0.897,
        "window=20": 0.856,
        "window=50": 0.734
      }
    },
    "cache_effectiveness": {
      "computation_time_saved_ms": 45678.9,
      "cache_overhead_ms": 234.5,
      "net_performance_gain_ms": 45444.4,
      "effectiveness_rating": "high"
    }
  }
}
```

### Detailed Cache Entries (when details=true)
```json
{
  "status": "success",
  "cache_entries": [
    {
      "cache_key": "sma_window=10_hash=a1b2c3d4",
      "function": "sma",
      "parameters": {
        "window": 10
      },
      "data_hash": "a1b2c3d4e5f6",
      "entry_size_kb": 32.7,
      "created_at": 1709337400000000000,
      "last_accessed": 1709337580000000000,
      "access_count": 15,
      "hit_rate": 0.88,
      "ttl_remaining_seconds": 1847,
      "eviction_priority": 0.73
    },
    {
      "cache_key": "derivative_method=central_hash=f6e5d4c3",
      "function": "derivative",
      "parameters": {
        "method": "central",
        "time_unit": "seconds"
      },
      "data_hash": "f6e5d4c3b2a1",
      "entry_size_kb": 48.2,
      "created_at": 1709337200000000000,
      "last_accessed": 1709337550000000000,
      "access_count": 7,
      "hit_rate": 0.64,
      "ttl_remaining_seconds": 1647,
      "eviction_priority": 0.45
    }
  ],
  "total_entries": 2847,
  "page_size": 2,
  "has_more": true
}
```

### Cache Performance Trends
```json
{
  "status": "success",
  "timeframe": "7d",
  "trends": {
    "daily_statistics": [
      {
        "date": "2024-03-01",
        "hit_rate": 0.834,
        "total_requests": 48567,
        "cache_size_mb": 76.3,
        "evictions": 89
      },
      {
        "date": "2024-03-02", 
        "hit_rate": 0.841,
        "total_requests": 52103,
        "cache_size_mb": 81.7,
        "evictions": 94
      },
      {
        "date": "2024-03-03",
        "hit_rate": 0.847,
        "total_requests": 53394,
        "cache_size_mb": 89.7,
        "evictions": 156
      }
    ],
    "trend_analysis": {
      "hit_rate": {
        "direction": "improving",
        "change_percent": 1.6,
        "significance": "low"
      },
      "cache_size": {
        "direction": "increasing",
        "change_percent": 17.5,
        "significance": "high"
      },
      "eviction_rate": {
        "direction": "increasing",
        "change_percent": 75.3,
        "significance": "high"
      }
    },
    "optimization_analysis": {
      "cache_pressure": "medium",
      "recommended_cache_size_mb": 160.0,
      "predicted_hit_rate_improvement": 0.12,
      "cost_benefit_ratio": 3.4
    }
  }
}
```

### Cache Management Information
```json
{
  "status": "success",
  "cache_management": {
    "configuration": {
      "max_cache_size_mb": 128.0,
      "default_ttl_seconds": 3600,
      "eviction_policy": "lru",
      "cleanup_interval_seconds": 300,
      "max_entry_size_mb": 50.0
    },
    "operational_status": {
      "cache_enabled": true,
      "last_cleanup": 1709337580000000000,
      "next_cleanup": 1709337880000000000,
      "maintenance_mode": false,
      "health_status": "healthy"
    },
    "management_operations": {
      "manual_invalidations_24h": 22,
      "cache_flushes_24h": 0,
      "configuration_changes_24h": 1,
      "maintenance_events_24h": 0
    },
    "cache_policies": {
      "per_function_ttl": {
        "sma": 3600,
        "derivative": 1800,
        "scale": 7200,
        "default": 3600
      },
      "size_limits": {
        "per_function_max_mb": 32.0,
        "per_entry_max_mb": 50.0,
        "global_max_mb": 128.0
      }
    }
  }
}
```

## Usage Examples

### Get Overall Cache Statistics
```bash
# Complete cache overview
curl -s "http://localhost:8086/functions/cache" | jq

# Cache performance metrics only
curl -s "http://localhost:8086/functions/cache" | jq '.performance_metrics'

# Memory statistics
curl -s "http://localhost:8086/functions/cache" | jq '.memory_statistics'
```

### Get Function-Specific Cache Data
```bash
# Cache statistics for SMA function
curl -s "http://localhost:8086/functions/cache?function=sma" | jq

# Hit rate for derivative function
curl -s "http://localhost:8086/functions/cache?function=derivative" | jq '.cache_statistics.basic_metrics.hit_rate'
```

### Get Detailed Cache Information
```bash
# Detailed cache entries (first page)
curl -s "http://localhost:8086/functions/cache?details=true" | jq '.cache_entries[]'

# Cache trends over last week
curl -s "http://localhost:8086/functions/cache?timeframe=7d" | jq '.trends'
```

## Python Client Example

```python
import requests
import json
import time
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta

class TSDBFunctionCacheClient:
    def __init__(self, host="localhost", port=8086):
        self.base_url = f"http://{host}:{port}"
    
    def get_cache_statistics(self, function: Optional[str] = None, 
                           metric: Optional[str] = None,
                           timeframe: str = "24h", 
                           details: bool = False) -> Dict[str, Any]:
        """Get function cache statistics."""
        params = {}
        if function:
            params['function'] = function
        if metric:
            params['metric'] = metric
        if timeframe != "24h":
            params['timeframe'] = timeframe
        if details:
            params['details'] = 'true'
        
        response = requests.get(f"{self.base_url}/functions/cache", params=params)
        return response.json()
    
    def get_cache_overview(self) -> Dict[str, Any]:
        """Get overall cache performance overview."""
        result = self.get_cache_statistics()
        
        if result.get("status") != "success":
            return {}
        
        return {
            "cache_overview": result.get("cache_overview", {}),
            "performance_metrics": result.get("performance_metrics", {}),
            "memory_statistics": result.get("memory_statistics", {})
        }
    
    def get_function_cache_stats(self, function_name: str) -> Dict[str, Any]:
        """Get cache statistics for a specific function."""
        result = self.get_cache_statistics(function=function_name)
        
        if result.get("status") != "success":
            return {}
        
        return result.get("cache_statistics", {})
    
    def get_cache_efficiency_report(self) -> Dict[str, Any]:
        """Generate cache efficiency analysis."""
        result = self.get_cache_statistics()
        
        if result.get("status") != "success":
            return {"error": "Failed to fetch cache data"}
        
        performance = result.get("performance_metrics", {})
        memory = result.get("memory_statistics", {})
        overview = result.get("cache_overview", {})
        
        # Calculate efficiency scores
        hit_rate = performance.get("overall_hit_rate", 0)
        utilization = overview.get("cache_utilization_percent", 0) / 100
        fragmentation = memory.get("fragmentation_percent", 0) / 100
        
        efficiency_score = (hit_rate * 0.5 + utilization * 0.3 + (1 - fragmentation) * 0.2) * 10
        
        return {
            "efficiency_score": efficiency_score,
            "hit_rate": hit_rate,
            "memory_utilization": utilization,
            "fragmentation": fragmentation,
            "recommendations": self._generate_cache_recommendations(result)
        }
    
    def _generate_cache_recommendations(self, cache_data: Dict[str, Any]) -> List[str]:
        """Generate cache optimization recommendations."""
        recommendations = []
        
        performance = cache_data.get("performance_metrics", {})
        memory = cache_data.get("memory_statistics", {})
        evictions = cache_data.get("eviction_statistics", {})
        
        hit_rate = performance.get("overall_hit_rate", 0)
        utilization = cache_data.get("cache_overview", {}).get("cache_utilization_percent", 0)
        eviction_rate = evictions.get("eviction_rate_per_hour", 0)
        
        if hit_rate < 0.7:
            recommendations.append("Low hit rate detected. Consider increasing cache size or adjusting TTL values.")
        
        if utilization > 90:
            recommendations.append("High memory utilization. Consider increasing cache size to reduce pressure.")
        
        if eviction_rate > 10:
            recommendations.append("High eviction rate detected. Increase cache size or optimize query patterns.")
        
        if memory.get("fragmentation_percent", 0) > 15:
            recommendations.append("High memory fragmentation. Consider cache cleanup or restart.")
        
        if not recommendations:
            recommendations.append("Cache performance is optimal. No immediate action required.")
        
        return recommendations
    
    def get_cache_trends(self, timeframe: str = "7d") -> Dict[str, Any]:
        """Get cache performance trends over time."""
        result = self.get_cache_statistics(timeframe=timeframe)
        
        if result.get("status") != "success":
            return {}
        
        return result.get("trends", {})
    
    def get_top_cached_functions(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get functions with best cache performance."""
        result = self.get_cache_statistics()
        
        if result.get("status") != "success":
            return []
        
        functions = result.get("function_breakdown", [])
        
        # Sort by cache effectiveness and hit rate
        sorted_functions = sorted(functions, 
            key=lambda x: (x.get("hit_rate", 0), -x.get("evictions_24h", 0)), 
            reverse=True)
        
        return sorted_functions[:limit]
    
    def get_cache_problems(self) -> List[Dict[str, Any]]:
        """Identify cache-related problems and issues."""
        result = self.get_cache_statistics()
        
        if result.get("status") != "success":
            return [{"issue": "Unable to fetch cache data", "severity": "critical"}]
        
        problems = []
        
        performance = result.get("performance_metrics", {})
        memory = result.get("memory_statistics", {})
        evictions = result.get("eviction_statistics", {})
        functions = result.get("function_breakdown", [])
        
        # Check overall hit rate
        hit_rate = performance.get("overall_hit_rate", 0)
        if hit_rate < 0.5:
            problems.append({
                "issue": f"Very low cache hit rate: {hit_rate:.1%}",
                "severity": "critical",
                "recommendation": "Investigate query patterns and consider cache configuration changes"
            })
        elif hit_rate < 0.7:
            problems.append({
                "issue": f"Low cache hit rate: {hit_rate:.1%}",
                "severity": "warning",
                "recommendation": "Consider increasing cache size or adjusting TTL values"
            })
        
        # Check memory pressure
        utilization = result.get("cache_overview", {}).get("cache_utilization_percent", 0)
        if utilization > 95:
            problems.append({
                "issue": f"Very high memory utilization: {utilization:.1f}%",
                "severity": "critical",
                "recommendation": "Increase cache size immediately to prevent performance degradation"
            })
        elif utilization > 85:
            problems.append({
                "issue": f"High memory utilization: {utilization:.1f}%",
                "severity": "warning",
                "recommendation": "Consider increasing cache size"
            })
        
        # Check eviction rate
        eviction_rate = evictions.get("eviction_rate_per_hour", 0)
        if eviction_rate > 50:
            problems.append({
                "issue": f"Very high eviction rate: {eviction_rate:.1f} per hour",
                "severity": "critical",
                "recommendation": "Increase cache size or optimize query patterns"
            })
        elif eviction_rate > 20:
            problems.append({
                "issue": f"High eviction rate: {eviction_rate:.1f} per hour",
                "severity": "warning",
                "recommendation": "Monitor cache pressure and consider size increase"
            })
        
        # Check individual function problems
        for func in functions:
            func_name = func.get("function", "unknown")
            func_hit_rate = func.get("hit_rate", 0)
            func_evictions = func.get("evictions_24h", 0)
            
            if func_hit_rate < 0.3:
                problems.append({
                    "issue": f"Function '{func_name}' has very low hit rate: {func_hit_rate:.1%}",
                    "severity": "warning",
                    "recommendation": f"Investigate {func_name} usage patterns or disable caching"
                })
            
            if func_evictions > 100:
                problems.append({
                    "issue": f"Function '{func_name}' has high evictions: {func_evictions}",
                    "severity": "warning",
                    "recommendation": f"Consider dedicated cache space for {func_name}"
                })
        
        return problems
    
    def monitor_cache_health(self) -> Dict[str, Any]:
        """Monitor cache health and return status."""
        result = self.get_cache_statistics()
        
        if result.get("status") != "success":
            return {
                "health_status": "unhealthy",
                "issues": ["Unable to fetch cache statistics"],
                "recommendations": ["Check TSDB server connectivity"]
            }
        
        problems = self.get_cache_problems()
        efficiency = self.get_cache_efficiency_report()
        
        # Determine overall health
        critical_issues = [p for p in problems if p.get("severity") == "critical"]
        warning_issues = [p for p in problems if p.get("severity") == "warning"]
        
        if critical_issues:
            health_status = "unhealthy"
        elif warning_issues:
            health_status = "degraded"
        else:
            health_status = "healthy"
        
        return {
            "health_status": health_status,
            "efficiency_score": efficiency.get("efficiency_score", 0),
            "critical_issues": len(critical_issues),
            "warning_issues": len(warning_issues),
            "problems": problems[:5],  # Top 5 problems
            "recommendations": efficiency.get("recommendations", [])
        }
    
    def clear_function_cache(self, function_name: str) -> bool:
        """Clear cache for a specific function (if management API exists)."""
        # This would integrate with a cache management API
        # For demonstration purposes, we'll simulate the call
        print(f"Would clear cache for function: {function_name}")
        return True
    
    def get_cache_size_recommendation(self) -> Dict[str, Any]:
        """Recommend optimal cache size based on current usage."""
        result = self.get_cache_statistics()
        
        if result.get("status") != "success":
            return {"error": "Unable to analyze cache data"}
        
        current_size = result.get("cache_overview", {}).get("total_cache_size_mb", 0)
        utilization = result.get("cache_overview", {}).get("cache_utilization_percent", 0)
        hit_rate = result.get("performance_metrics", {}).get("overall_hit_rate", 0)
        eviction_rate = result.get("eviction_statistics", {}).get("eviction_rate_per_hour", 0)
        
        # Simple recommendation algorithm
        if utilization > 90 and eviction_rate > 20:
            recommended_size = current_size * 1.5
            reason = "High utilization and eviction rate"
        elif utilization > 80 and hit_rate < 0.8:
            recommended_size = current_size * 1.3
            reason = "Moderate pressure affecting hit rate"
        elif utilization < 50:
            recommended_size = current_size * 0.8
            reason = "Low utilization - can reduce size"
        else:
            recommended_size = current_size
            reason = "Current size is appropriate"
        
        return {
            "current_size_mb": current_size,
            "recommended_size_mb": recommended_size,
            "size_change_percent": ((recommended_size - current_size) / current_size) * 100 if current_size > 0 else 0,
            "reason": reason,
            "expected_benefits": self._calculate_benefits(current_size, recommended_size, hit_rate)
        }
    
    def _calculate_benefits(self, current_size: float, recommended_size: float, current_hit_rate: float) -> List[str]:
        """Calculate expected benefits from cache size change."""
        benefits = []
        size_ratio = recommended_size / current_size if current_size > 0 else 1
        
        if size_ratio > 1.2:
            estimated_hit_rate_improvement = min(0.15, (size_ratio - 1) * 0.3)
            benefits.append(f"Estimated hit rate improvement: +{estimated_hit_rate_improvement:.1%}")
            benefits.append("Reduced eviction pressure")
            benefits.append("Better performance for complex functions")
        elif size_ratio < 0.9:
            benefits.append("Reduced memory usage")
            benefits.append("Lower resource overhead")
        else:
            benefits.append("Maintains current performance")
        
        return benefits

# Usage Examples
client = TSDBFunctionCacheClient()

# Get cache overview
overview = client.get_cache_overview()
cache_overview = overview.get("cache_overview", {})
performance_metrics = overview.get("performance_metrics", {})

print("Cache Overview:")
print(f"  Total cache size: {cache_overview.get('total_cache_size_mb', 0):.1f}MB")
print(f"  Used cache size: {cache_overview.get('used_cache_size_mb', 0):.1f}MB")
print(f"  Cache utilization: {cache_overview.get('cache_utilization_percent', 0):.1f}%")
print(f"  Hit rate: {performance_metrics.get('overall_hit_rate', 0):.1%}")
print(f"  Total entries: {cache_overview.get('total_cache_entries', 0):,}")

# Get cache efficiency report
efficiency = client.get_cache_efficiency_report()
print(f"\nCache Efficiency:")
print(f"  Efficiency score: {efficiency.get('efficiency_score', 0):.1f}/10")
print(f"  Hit rate: {efficiency.get('hit_rate', 0):.1%}")
print(f"  Memory utilization: {efficiency.get('memory_utilization', 0):.1%}")
print(f"  Fragmentation: {efficiency.get('fragmentation', 0):.1%}")

print(f"\nRecommendations:")
for rec in efficiency.get("recommendations", []):
    print(f"  • {rec}")

# Get function-specific cache statistics
sma_stats = client.get_function_cache_stats("sma")
if sma_stats:
    basic_metrics = sma_stats.get("basic_metrics", {})
    print(f"\nSMA Cache Statistics:")
    print(f"  Hit rate: {basic_metrics.get('hit_rate', 0):.1%}")
    print(f"  Total entries: {basic_metrics.get('total_entries', 0):,}")
    print(f"  Memory usage: {sma_stats.get('memory_metrics', {}).get('total_memory_mb', 0):.1f}MB")

# Get top performing functions
top_functions = client.get_top_cached_functions(5)
print(f"\nTop Cached Functions:")
for i, func in enumerate(top_functions, 1):
    print(f"  {i}. {func.get('function', 'unknown')}: {func.get('hit_rate', 0):.1%} hit rate")

# Check for cache problems
problems = client.get_cache_problems()
if problems:
    print(f"\nCache Problems:")
    for problem in problems:
        severity = problem.get("severity", "unknown").upper()
        issue = problem.get("issue", "No description")
        print(f"  [{severity}] {issue}")
        if problem.get("recommendation"):
            print(f"      → {problem['recommendation']}")
else:
    print(f"\n✓ No cache problems detected")

# Monitor cache health
health = client.monitor_cache_health()
print(f"\nCache Health: {health.get('health_status', 'unknown').upper()}")
print(f"Efficiency Score: {health.get('efficiency_score', 0):.1f}/10")
print(f"Issues: {health.get('critical_issues', 0)} critical, {health.get('warning_issues', 0)} warnings")

# Get size recommendation
size_rec = client.get_cache_size_recommendation()
current_mb = size_rec.get('current_size_mb', 0)
recommended_mb = size_rec.get('recommended_size_mb', 0)
change_percent = size_rec.get('size_change_percent', 0)

print(f"\nCache Size Recommendation:")
print(f"  Current: {current_mb:.1f}MB")
print(f"  Recommended: {recommended_mb:.1f}MB ({change_percent:+.1f}%)")
print(f"  Reason: {size_rec.get('reason', 'No reason provided')}")

benefits = size_rec.get('expected_benefits', [])
if benefits:
    print(f"  Expected benefits:")
    for benefit in benefits:
        print(f"    • {benefit}")

# Get cache trends
trends = client.get_cache_trends("7d")
if trends:
    trend_analysis = trends.get("trend_analysis", {})
    print(f"\nCache Trends (7 days):")
    for metric, trend_data in trend_analysis.items():
        direction = trend_data.get("direction", "unknown")
        change = trend_data.get("change_percent", 0)
        significance = trend_data.get("significance", "unknown")
        print(f"  {metric}: {direction} ({change:+.1f}%) - {significance} significance")
```

## Advanced Cache Management

### Cache Monitoring Dashboard
```python
def create_cache_dashboard(client):
    """Create a real-time cache monitoring dashboard."""
    import time
    
    while True:
        # Clear screen
        print("\033[2J\033[H")
        
        print("TSDB Function Cache Dashboard")
        print("=" * 50)
        print(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print()
        
        # Get current cache statistics
        overview = client.get_cache_overview()
        
        if overview:
            cache_stats = overview.get("cache_overview", {})
            performance = overview.get("performance_metrics", {})
            memory = overview.get("memory_statistics", {})
            
            # Cache overview
            print("Cache Overview:")
            print(f"  Size: {cache_stats.get('used_cache_size_mb', 0):.1f}/{cache_stats.get('total_cache_size_mb', 0):.1f} MB")
            print(f"  Utilization: {cache_stats.get('cache_utilization_percent', 0):.1f}%")
            print(f"  Entries: {cache_stats.get('total_cache_entries', 0):,}")
            print(f"  Efficiency Score: {cache_stats.get('cache_efficiency_score', 0):.1f}/10")
            print()
            
            # Performance metrics
            print("Performance (24h):")
            hit_rate = performance.get('overall_hit_rate', 0)
            total_requests = performance.get('total_requests_24h', 0)
            print(f"  Hit Rate: {hit_rate:.1%}")
            print(f"  Requests: {total_requests:,}")
            print(f"  Time Saved: {performance.get('cache_save_time_ms', 0) / 1000:.1f}s")
            print()
            
            # Memory details
            print("Memory:")
            print(f"  Fragmentation: {memory.get('fragmentation_percent', 0):.1f}%")
            print(f"  Avg Entry Size: {memory.get('average_entry_size_kb', 0):.1f}KB")
            print(f"  Largest Entry: {memory.get('largest_entry_mb', 0):.1f}MB")
            print()
            
            # Health status
            health = client.monitor_cache_health()
            status = health.get("health_status", "unknown").upper()
            status_color = "🟢" if status == "HEALTHY" else "🟡" if status == "DEGRADED" else "🔴"
            print(f"Health Status: {status_color} {status}")
            
            # Show problems if any
            problems = health.get("problems", [])
            if problems:
                print("\nIssues:")
                for problem in problems[:3]:  # Show top 3
                    severity = problem.get("severity", "unknown").upper()
                    issue = problem.get("issue", "No description")
                    print(f"  [{severity}] {issue}")
        else:
            print("✗ Unable to fetch cache data")
        
        print("\nPress Ctrl+C to exit")
        
        try:
            time.sleep(10)  # Update every 10 seconds
        except KeyboardInterrupt:
            print("\nDashboard stopped.")
            break

# Usage (run in interactive environment)
# create_cache_dashboard(client)
```

### Cache Optimization Tools
```python
def optimize_cache_configuration(client):
    """Analyze and suggest cache configuration optimizations."""
    print("Cache Configuration Optimization Analysis")
    print("=" * 50)
    
    # Get current state
    overview = client.get_cache_overview()
    problems = client.get_cache_problems()
    size_rec = client.get_cache_size_recommendation()
    
    print("\nCurrent Configuration Analysis:")
    print(f"Cache Size: {size_rec.get('current_size_mb', 0):.1f}MB")
    
    # Analyze problems by severity
    critical_problems = [p for p in problems if p.get("severity") == "critical"]
    warning_problems = [p for p in problems if p.get("severity") == "warning"]
    
    if critical_problems:
        print(f"\n🔴 CRITICAL ISSUES ({len(critical_problems)}):")
        for problem in critical_problems:
            print(f"  • {problem.get('issue', 'Unknown issue')}")
            print(f"    → {problem.get('recommendation', 'No recommendation')}")
    
    if warning_problems:
        print(f"\n🟡 WARNINGS ({len(warning_problems)}):")
        for problem in warning_problems[:3]:  # Limit to 3 warnings
            print(f"  • {problem.get('issue', 'Unknown issue')}")
    
    # Size recommendations
    print(f"\n💡 SIZE RECOMMENDATION:")
    current_mb = size_rec.get('current_size_mb', 0)
    recommended_mb = size_rec.get('recommended_size_mb', 0)
    change_percent = size_rec.get('size_change_percent', 0)
    
    if abs(change_percent) > 10:
        print(f"  Resize cache: {current_mb:.1f}MB → {recommended_mb:.1f}MB ({change_percent:+.1f}%)")
        print(f"  Reason: {size_rec.get('reason', 'No reason provided')}")
    else:
        print(f"  Current size ({current_mb:.1f}MB) is appropriate")
    
    # Function-specific recommendations
    top_functions = client.get_top_cached_functions(10)
    low_performance_functions = [f for f in top_functions if f.get("hit_rate", 0) < 0.5]
    
    if low_performance_functions:
        print(f"\n⚠ FUNCTIONS WITH LOW CACHE EFFECTIVENESS:")
        for func in low_performance_functions:
            name = func.get('function', 'unknown')
            hit_rate = func.get('hit_rate', 0)
            print(f"  {name}: {hit_rate:.1%} hit rate - consider disabling cache or adjusting parameters")

# Usage
optimize_cache_configuration(client)
```

## Error Handling

### Common Error Scenarios
- **Cache System Disabled**: Function caching is not enabled
- **Insufficient Permissions**: Cannot access cache statistics
- **Function Not Found**: Specified function not found in cache data
- **Invalid Timeframe**: Unsupported time period format

### Robust Client Implementation
```python
def get_cache_stats_safely(client, function_name=None):
    """Get cache statistics with comprehensive error handling."""
    try:
        result = client.get_cache_statistics(function=function_name)
        
        if result.get("status") != "success":
            error_msg = result.get("error", "Unknown error")
            print(f"Cache statistics request failed: {error_msg}")
            return None
        
        return result
        
    except requests.exceptions.ConnectionError:
        print("Failed to connect to TSDB server")
        return None
    except requests.exceptions.Timeout:
        print("Cache statistics request timed out")
        return None
    except Exception as e:
        print(f"Unexpected error: {e}")
        return None
```

## Related Endpoints

- [`GET /functions`](functions-endpoint.md) - List functions that use caching
- [`GET /functions/performance`](functions-performance-endpoint.md) - Performance data includes cache impact
- [`POST /query/functions`](query-functions-endpoint.md) - Execute function queries that use cache
- [`POST /functions/validate`](functions-validate-endpoint.md) - Validate before caching results