import time
import json
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional
from agentic_chunker import AgenticChunker

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("ChunkingMonitor")

class ChunkingPerformanceMonitor:
    """
    Monitors and optimizes chunking performance based on retrieval metrics.
    """
    def __init__(self, agentic_chunker: AgenticChunker, log_file: str = "chunking_metrics.json"):
        """
        Initialize the performance monitor.
        
        Args:
            agentic_chunker: The AgenticChunker instance to monitor and optimize
            log_file: Path to file where metrics will be logged
        """
        self.chunker = agentic_chunker
        self.log_file = log_file
        self.metrics_history = []
        self.last_adaptation_time = datetime.now()
        
        # Performance threshold constants
        self.min_relevance_threshold = 0.7  # Minimum acceptable relevance score
        self.max_latency_threshold = 5.0    # Maximum acceptable query latency in seconds
        self.adaptation_cooldown = 60*60    # Wait 1 hour between adaptations
        
        # Load existing metrics if file exists
        try:
            with open(self.log_file, 'r') as f:
                self.metrics_history = json.load(f)
            logger.info(f"Loaded {len(self.metrics_history)} historical metrics records")
        except (FileNotFoundError, json.JSONDecodeError):
            logger.info("No existing metrics history found, starting fresh")
    
    def log_query_metrics(self, 
                         query: str, 
                         chunks_retrieved: int,
                         response_latency: float,
                         context_relevance: Optional[float] = None,
                         user_feedback: Optional[str] = None,
                         content_type: Optional[str] = None) -> Dict[str, Any]:
        """
        Log metrics for a query and its results.
        
        Args:
            query: The user query
            chunks_retrieved: Number of chunks retrieved
            response_latency: Time taken to process the query (seconds)
            context_relevance: Relevance score (0.0-1.0) if available
            user_feedback: Optional user feedback about result quality
            content_type: Content type of the retrieved documents
            
        Returns:
            The created metrics record
        """
        metrics = {
            "timestamp": datetime.now().isoformat(),
            "query": query,
            "query_length": len(query),
            "chunks_retrieved": chunks_retrieved,
            "response_latency": response_latency,
            "chunking_config": {
                "default_chunk_size": self.chunker.default_chunk_size,
                "default_chunk_overlap": self.chunker.default_chunk_overlap
            }
        }
        
        # Add optional metrics if available
        if context_relevance is not None:
            metrics["context_relevance"] = context_relevance
            
        if user_feedback:
            metrics["user_feedback"] = user_feedback
            
        if content_type:
            metrics["content_type"] = content_type
        
        # Append to history and save
        self.metrics_history.append(metrics)
        self._save_metrics()
        
        # Evaluate if we should adapt the chunking strategy
        self._evaluate_adaptation()
        
        return metrics
    
    def _save_metrics(self):
        """Save metrics history to the log file."""
        try:
            with open(self.log_file, 'w') as f:
                json.dump(self.metrics_history, f, indent=2)
        except Exception as e:
            logger.error(f"Error saving metrics to {self.log_file}: {str(e)}")
    
    def _evaluate_adaptation(self):
        """Evaluate whether to adapt the chunking strategy based on recent metrics."""
        # Check if enough time has passed since last adaptation
        time_since_last = (datetime.now() - self.last_adaptation_time).total_seconds()
        if time_since_last < self.adaptation_cooldown:
            return
        
        # Only adapt if we have enough metrics
        if len(self.metrics_history) < 5:
            return
        
        # Analyze recent metrics (last 20 or fewer)
        recent_metrics = self.metrics_history[-20:]
        
        # Calculate average relevance and latency
        relevance_scores = [m.get("context_relevance", None) for m in recent_metrics if "context_relevance" in m]
        latencies = [m.get("response_latency", None) for m in recent_metrics if "response_latency" in m]
        
        if not relevance_scores or not latencies:
            return
        
        avg_relevance = sum(relevance_scores) / len(relevance_scores)
        avg_latency = sum(latencies) / len(latencies)
        
        logger.info(f"Recent performance metrics - Avg relevance: {avg_relevance:.2f}, Avg latency: {avg_latency:.2f}s")
        
        # Decide if adaptation is needed
        adaptation_metrics = {}
        
        if avg_relevance < self.min_relevance_threshold:
            adaptation_metrics["context_relevance"] = avg_relevance
        
        if avg_latency > self.max_latency_threshold:
            adaptation_metrics["query_latency"] = avg_latency
        
        # Apply adaptation if needed
        if adaptation_metrics:
            logger.info(f"Adapting chunking strategy based on metrics: {adaptation_metrics}")
            
            # Record current settings for comparison
            old_size = self.chunker.default_chunk_size
            old_overlap = self.chunker.default_chunk_overlap
            
            # Adapt chunking strategy
            self.chunker.adapt_chunking_strategy(adaptation_metrics)
            
            # Log the changes
            logger.info(f"Chunking parameters adjusted: size {old_size} → {self.chunker.default_chunk_size}, "
                       f"overlap {old_overlap} → {self.chunker.default_chunk_overlap}")
            
            # Update adaptation timestamp
            self.last_adaptation_time = datetime.now()
    
    def get_content_type_performance(self) -> Dict[str, Dict[str, float]]:
        """
        Analyze performance metrics by content type.
        
        Returns:
            Dictionary mapping content types to performance metrics
        """
        content_metrics = {}
        
        # Group metrics by content type
        for metric in self.metrics_history:
            content_type = metric.get("content_type")
            if not content_type:
                continue
                
            if content_type not in content_metrics:
                content_metrics[content_type] = {
                    "relevance_scores": [],
                    "latencies": [],
                    "counts": 0
                }
            
            if "context_relevance" in metric:
                content_metrics[content_type]["relevance_scores"].append(metric["context_relevance"])
                
            if "response_latency" in metric:
                content_metrics[content_type]["latencies"].append(metric["response_latency"])
                
            content_metrics[content_type]["counts"] += 1
        
        # Calculate averages
        results = {}
        for content_type, data in content_metrics.items():
            avg_relevance = sum(data["relevance_scores"]) / len(data["relevance_scores"]) if data["relevance_scores"] else None
            avg_latency = sum(data["latencies"]) / len(data["latencies"]) if data["latencies"] else None
            
            results[content_type] = {
                "avg_relevance": avg_relevance,
                "avg_latency": avg_latency,
                "query_count": data["counts"]
            }
        
        return results
    
    def analyze_performance_trends(self, window_size: int = 10) -> Dict[str, List[float]]:
        """
        Analyze performance trends over time using moving averages.
        
        Args:
            window_size: Size of the moving average window
            
        Returns:
            Dictionary with trend data
        """
        if len(self.metrics_history) < window_size:
            return {"message": f"Insufficient data. Need at least {window_size} data points."}
        
        # Extract timestamps, relevance, and latency
        timestamps = []
        relevance_trend = []
        latency_trend = []
        
        for i in range(len(self.metrics_history) - window_size + 1):
            window = self.metrics_history[i:i+window_size]
            
            # Calculate window averages
            rel_scores = [m.get("context_relevance", None) for m in window if "context_relevance" in m]
            lats = [m.get("response_latency", None) for m in window if "response_latency" in m]
            
            if rel_scores:
                avg_rel = sum(rel_scores) / len(rel_scores)
                relevance_trend.append(avg_rel)
            else:
                relevance_trend.append(None)
                
            if lats:
                avg_lat = sum(lats) / len(lats)
                latency_trend.append(avg_lat)
            else:
                latency_trend.append(None)
                
            # Use the last timestamp in the window
            timestamps.append(window[-1]["timestamp"])
        
        return {
            "timestamps": timestamps,
            "relevance_trend": relevance_trend,
            "latency_trend": latency_trend
        }
    
    def recommend_chunking_strategy(self) -> Dict[str, Any]:
        """
        Recommend optimal chunking strategies based on accumulated metrics.
        
        Returns:
            Dictionary with recommended settings
        """
        if not self.metrics_history:
            return {"message": "Insufficient data for recommendations"}
        
        # Analyze performance by content type
        content_performance = self.get_content_type_performance()
        
        # Make recommendations
        recommendations = {
            "general": {
                "current_chunk_size": self.chunker.default_chunk_size,
                "current_chunk_overlap": self.chunker.default_chunk_overlap,
            },
            "content_type_recommendations": {}
        }
        
        # Make content-specific recommendations
        for content_type, metrics in content_performance.items():
            if metrics["avg_relevance"] is None or metrics["avg_latency"] is None:
                continue
                
            recommended_size = self.chunker.default_chunk_size
            recommended_overlap = self.chunker.default_chunk_overlap
            
            # If relevance is low, reduce chunk size and increase overlap
            if metrics["avg_relevance"] < 0.7:
                recommended_size = max(5000, recommended_size - 2000)
                recommended_overlap = min(500, recommended_overlap + 100)
            
            # If latency is high, increase chunk size to reduce vector count
            if metrics["avg_latency"] > 5.0:
                recommended_size = min(15000, recommended_size + 2000)
            
            recommendations["content_type_recommendations"][content_type] = {
                "recommended_chunk_size": recommended_size,
                "recommended_chunk_overlap": recommended_overlap,
                "avg_relevance": metrics["avg_relevance"],
                "avg_latency": metrics["avg_latency"],
                "query_count": metrics["query_count"]
            }
        
        return recommendations