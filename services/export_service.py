"""
Export Service for Analytics Data.

Handles CSV and PDF generation for analytics exports.
"""

import csv
import io
from typing import List, Dict, Any
from datetime import date, datetime
from fastapi.responses import StreamingResponse, Response
from jinja2 import Environment, FileSystemLoader, select_autoescape
import structlog

logger = structlog.get_logger(__name__)


class CSVExportService:
    """Service for exporting analytics data to CSV format."""
    
    @staticmethod
    def export_daily_metrics_csv(metrics: List[Dict[str, Any]]) -> StreamingResponse:
        """
        Export daily metrics to CSV format.
        
        Args:
            metrics: List of daily metrics dictionaries
            
        Returns:
            StreamingResponse with CSV content
        """
        output = io.StringIO()
        
        if not metrics:
            # Return empty CSV with headers
            writer = csv.DictWriter(output, fieldnames=[
                'date', 'focus_score', 'deep_work_hours', 'context_switches',
                'meeting_load_pct', 'distraction_index', 'break_quality'
            ])
            writer.writeheader()
            output.seek(0)
            
            return StreamingResponse(
                iter([output.getvalue()]),
                media_type="text/csv",
                headers={"Content-Disposition": "attachment; filename=daily_metrics.csv"}
            )
        
        # Get fieldnames from first metric
        fieldnames = ['date', 'focus_score', 'deep_work_hours', 'context_switches',
                     'meeting_load_pct', 'distraction_index', 'break_quality']
        
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        
        for metric in metrics:
            writer.writerow({
                'date': metric.get('date', ''),
                'focus_score': float(metric.get('focus_score') or 0),
                'deep_work_hours': float(metric.get('deep_work_hours') or 0),
                'context_switches': int(metric.get('context_switches') or 0),
                'meeting_load_pct': float(metric.get('meeting_load_pct') or 0),
                'distraction_index': float(metric.get('distraction_index') or 0),
                'break_quality': float(metric.get('break_quality') or 0)
            })
        
        output.seek(0)
        
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={
                "Content-Disposition": "attachment; filename=daily_metrics.csv"
            }
        )
    
    @staticmethod
    def export_weekly_metrics_csv(
        metrics: List[Dict[str, Any]],
        week_start: date
    ) -> StreamingResponse:
        """Export weekly metrics to CSV format."""
        output = io.StringIO()
        
        fieldnames = ['week_start', 'week_end', 'avg_focus_score', 
                     'total_deep_work_hours', 'avg_meeting_load', 
                     'total_context_switches', 'days_tracked']
        
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        
        for metric in metrics:
            writer.writerow({
                'week_start': metric.get('week_start', ''),
                'week_end': metric.get('week_end', ''),
                'avg_focus_score': float(metric.get('avg_focus_score') or 0),
                'total_deep_work_hours': float(metric.get('total_deep_work_hours') or 0),
                'avg_meeting_load': float(metric.get('avg_meeting_load') or 0),
                'total_context_switches': int(metric.get('total_context_switches') or 0),
                'days_tracked': int(metric.get('days_tracked') or 0)
            })
        
        output.seek(0)
        
        filename = f"weekly_metrics_{week_start}.csv"
        
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )


class PDFExportService:
    """Service for exporting analytics reports to PDF format."""
    
    def __init__(self, template_dir: str = "backend/templates"):
        """Initialize PDF export service with Jinja2 templates."""
        self.env = Environment(
            loader=FileSystemLoader(template_dir),
            autoescape=select_autoescape(['html', 'xml'])
        )
        logger.info("PDF export service initialized", template_dir=template_dir)
    
    async def export_weekly_report_pdf(
        self,
        report_data: Dict[str, Any]
    ) -> bytes:
        """
        Generate PDF from weekly report data.
        
        Args:
            report_data: Weekly report dictionary
            
        Returns:
            PDF bytes
        """
        try:
            # Lazy import weasyprint (heavy dependency)
            from weasyprint import HTML, CSS
            from weasyprint.text.fonts import FontConfiguration
        except ImportError:
            logger.warning("weasyprint not installed, using HTML fallback")
            # Return HTML as bytes if weasyprint not available
            template = self.env.get_template('weekly_report_pdf.html')
            html_content = template.render(
                report=report_data,
                generated_at=datetime.utcnow().strftime('%B %d, %Y at %H:%M UTC')
            )
            return html_content.encode('utf-8')
        
        # Get template
        template = self.env.get_template('weekly_report_pdf.html')
        
        # Render HTML with data
        html_content = template.render(
            report=report_data,
            generated_at=datetime.utcnow().strftime('%B %d, %Y at %H:%M UTC')
        )
        
        # Generate PDF
        font_config = FontConfiguration()
        
        # Create PDF from HTML
        pdf_bytes = HTML(string=html_content).write_pdf(
            font_config=font_config
        )
        
        logger.info(
            "PDF generated successfully",
            report_week=report_data.get('week_start_date'),
            pdf_size=len(pdf_bytes)
        )
        
        return pdf_bytes
    
    async def export_daily_summary_pdf(
        self,
        summary_data: Dict[str, Any]
    ) -> bytes:
        """Generate PDF from daily summary data."""
        try:
            from weasyprint import HTML
        except ImportError:
            logger.warning("weasyprint not installed, using HTML fallback")
            template = self.env.get_template('daily_summary_pdf.html')
            html_content = template.render(
                summary=summary_data,
                generated_at=datetime.utcnow().strftime('%B %d, %Y at %H:%M UTC')
            )
            return html_content.encode('utf-8')
        
        template = self.env.get_template('daily_summary_pdf.html')
        
        html_content = template.render(
            summary=summary_data,
            generated_at=datetime.utcnow().strftime('%B %d, %Y at %H:%M UTC')
        )
        
        pdf_bytes = HTML(string=html_content).write_pdf()
        
        logger.info(
            "Daily summary PDF generated",
            date=summary_data.get('date'),
            pdf_size=len(pdf_bytes)
        )
        
        return pdf_bytes
