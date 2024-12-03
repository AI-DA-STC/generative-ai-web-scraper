import time
import requests
from typing import Dict, Any, Optional

from config import logger, schedule_config


class SlackHelper:
    """
    Helper class for Slack notifications and approval workflows.
    Handles sending messages with approval buttons and tracking responses.
    """
    
    def __init__(self):
        self.webhook_url = schedule_config.SLACK_WEBHOOK_URL
        self.channel = schedule_config.SLACK_CHANNEL
        self.api_token = schedule_config.SLACK_API_TOKEN  
    
    def send_notification(  
        self,
        changes: Dict[str, Any],
        timestamp: str
    ) -> str:
        """
        Send Slack notification with change summary and approval buttons.
        Returns message timestamp for tracking approval.
        
        Args:
            changes: Dictionary containing content changes
            timestamp: Version timestamp for reference
        
        Returns:
            Message timestamp for tracking responses
        """
        try:
            # Format change summary
            summary = self._format_change_summary(changes)
            
            # Create message payload
            payload = {
                "channel": self.channel,
                "text": "Web Scraper Update Approval Required",
                "blocks": [
                    {
                        "type": "header",
                        "text": {
                            "type": "plain_text",
                            "text": "Web Scraper Update Approval Required"
                        }
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"*Changes detected in scraping run {timestamp}:*\n{summary}"
                        }
                    },
                    {
                        "type": "actions",
                        "elements": [
                            {
                                "type": "button",
                                "text": {
                                    "type": "plain_text",
                                    "text": "✅ Approve Changes"
                                },
                                "style": "primary",
                                "value": "approve"
                            },
                            {
                                "type": "button",
                                "text": {
                                    "type": "plain_text",
                                    "text": "❌ Reject Changes"
                                },
                                "style": "danger",
                                "value": "reject"
                            }
                        ]
                    }
                ]
            }
            
            # Send message
            response = requests.post(
                self.webhook_url,
                json=payload,
                headers={"Content-Type": "application/json"}
            )
            response.raise_for_status()
            
            # Extract message timestamp
            message_data = response.json()
            return message_data.get("ts")
            
        except Exception as e:
            logger.error(f"Failed to send Slack notification: {str(e)}")
            raise
    
    def check_approval(
        self,
        message_ts: str,
        poll_interval: int = 60
    ) -> bool:
        """
        Check for user approval response via polling.
        Continuously polls until a button is clicked.
        
        Args:
            message_ts: Message timestamp to check
            poll_interval: Seconds to wait between polls
        
        Returns:
            True if approved, False if rejected
        """
        try:
            while True:
                response = self._get_message_response(message_ts)
                
                if response is not None:
                    # Update message with final status
                    self.update_message_status(message_ts, response)
                    return response
                
                # Wait before next poll
                time.sleep(poll_interval)
                
        except Exception as e:
            logger.error(f"Error checking approval status: {str(e)}")
            raise
    
    def _get_message_response(self, message_ts: str) -> Optional[bool]:
        """
        Get response from Slack message using Web API.
        
        Args:
            message_ts: Message timestamp to check
            
        Returns:
            True if approved, False if rejected, None if no response
        """
        try:
            # Get message replies/reactions using Slack Web API
            response = requests.get(
                'https://slack.com/api/conversations.history',
                params={
                    'channel': self.channel,
                    'latest': message_ts,
                    'limit': 1,
                    'inclusive': True
                },
                headers={
                    'Authorization': f'Bearer {self.api_token}'
                }
            )
            response.raise_for_status()
            data = response.json()
            
            if not data.get('ok'):
                raise Exception(f"Slack API error: {data.get('error')}")
            
            # Check if message has any button interactions
            message = data['messages'][0]
            if 'response_url' in message:
                # Button was clicked
                action = message.get('actions', [{}])[0].get('value')
                return action == 'approve'
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting message response: {str(e)}")
            raise
    
    def _format_change_summary(self, changes: Dict[str, Any]) -> str:
        """Format changes into readable Slack message."""
        summary = []
        
        # Add summary of added content
        if changes.get('added'):
            added = changes['added']
            summary.append(f"*Added Pages:* {len(added)}")
            for page in added[:5]:  # Show first 5
                summary.append(f"• {page['url']}")
            if len(added) > 5:
                summary.append(f"_...and {len(added)-5} more_")
        
        # Add summary of modified content
        if changes.get('modified'):
            modified = changes['modified']
            summary.append(f"\n*Modified Pages:* {len(modified)}")
            for page in modified[:5]:
                summary.append(f"• {page['url']}")
                if 'changes' in page:
                    for field, change in page['changes'].items():
                        summary.append(f"  - {field}: {change['old']} → {change['new']}")
            if len(modified) > 5:
                summary.append(f"_...and {len(modified)-5} more_")
        
        # Add summary of deleted content
        if changes.get('deleted'):
            deleted = changes['deleted']
            summary.append(f"\n*Deleted Pages:* {len(deleted)}")
            for page in deleted[:5]:
                summary.append(f"• {page['url']}")
            if len(deleted) > 5:
                summary.append(f"_...and {len(deleted)-5} more_")
        
        return "\n".join(summary)
    
    def update_message_status(
        self,
        message_ts: str,
        approved: bool
    ) -> None:
        """
        Update message to show final approval status.
        
        Args:
            message_ts: Message timestamp to update
            approved: Whether changes were approved
        """
        try:
            status = "✅ Approved" if approved else "❌ Rejected"
            
            # Update message
            payload = {
                "channel": self.channel,
                "ts": message_ts,
                "text": f"Update Status: {status}",
            }
            
            response = requests.post(
                self.webhook_url,
                json=payload,
                headers={"Content-Type": "application/json"}
            )
            response.raise_for_status()
            
        except Exception as e:
            logger.error(f"Failed to update message status: {str(e)}")
            raise