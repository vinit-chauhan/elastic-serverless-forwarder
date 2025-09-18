from ipaddress import IPv4Address
import unittest
import struct
from io import BytesIO
from share.json import json_parser
from share.ipfix_parser import parse_ipfix_stream
from processors.ipfix_ecs import ECSProcessor


class TestIPFIXECSIntegration(unittest.TestCase):
    """
    Test the integration of IPFIX processor with ECS processor.

    This test suite validates the complete pipeline from binary IPFIX data
    through parsing and ECS conversion, ensuring proper field mapping and
    data integrity throughout the process.
    """

    def setUp(self):
        """Set up test fixtures."""
        self.ecs_processor = ECSProcessor()

        # Common test data for consistency
        self.test_export_time = 1640995200
        self.test_sequence_number = 1
        self.test_domain_id = 1

    def create_simple_ipfix_stream(self) -> BytesIO:
        """Create a simple IPFIX file for testing"""
        # Template Set
        template_id = 256
        field_count = 2

        # Template Record: Template ID + Field Count + Field Specifiers
        template_record = struct.pack("!HH", template_id, field_count)

        # Field 1: sourceIPv4Address (ID=8, Length=4)
        field1 = struct.pack("!HH", 8, 4)

        # Field 2: destinationIPv4Address (ID=12, Length=4)
        field2 = struct.pack("!HH", 12, 4)

        template_data = template_record + field1 + field2
        template_set_length = 4 + len(template_data)  # 4 bytes for set header
        template_set_header = struct.pack("!HH", 2, template_set_length)  # Set ID=2 (Template)
        template_set = template_set_header + template_data

        # Data Set - Set ID=256 (matching template ID)
        # Data Record: sourceIP=192.168.1.1, destIP=10.0.0.1
        data_record = struct.pack("!II",
                                  int.from_bytes([192, 168, 1, 1], 'big'),  # sourceIPv4Address
                                  int.from_bytes([10, 0, 0, 1], 'big'))     # destinationIPv4Address

        data_set_length = 4 + len(data_record)  # 4 bytes for set header
        data_set_header = struct.pack("!HH", template_id, data_set_length)  # Set ID=256
        data_set = data_set_header + data_record

        # IPFIX Message Header
        message_length = 16 + len(template_set) + len(data_set)  # 16 bytes for message header
        header = struct.pack("!HHIII",
                             10,                      # Version=10
                             message_length,          # Length
                             self.test_export_time,   # Export Time
                             self.test_sequence_number,  # Sequence Number
                             self.test_domain_id)     # Observation Domain ID

        # Combine all parts
        ipfix_data = header + template_set + data_set
        return BytesIO(ipfix_data)

    def test_ipfix_to_ecs_pipeline(self):
        """Test that IPFIX data can be processed and converted to ECS format."""
        # Create a simple IPFIX message with template and data
        ipfix_data = self.create_simple_ipfix_stream()

        ipfix_result = parse_ipfix_stream(ipfix_data)

        # Get the first IPFIX record (parser returns tuples of (record, start_offset, end_offset))
        ipfix_record = None
        for record, _, _ in ipfix_result:
            ipfix_record = record
            break

        self.assertIsNotNone(ipfix_record, "Should have at least one IPFIX record")

        # Verify IPFIX record has expected fields
        self.assertIn('sourceIPv4Address', ipfix_record)
        self.assertIn('destinationIPv4Address', ipfix_record)

        # Verify header information is preserved
        if 'header' in ipfix_record:
            header = ipfix_record['header']
            self.assertEqual(header['export_time'], self.test_export_time)
            self.assertEqual(header['sequence_number'], self.test_sequence_number)

        # Process through ECS processor (wrap in expected format)
        wrapped_event = {
            "fields": {
                "message": ipfix_record
            }
        }
        ecs_result = self.ecs_processor.process(wrapped_event)

        self.assertFalse(ecs_result.is_empty, "ECS processor should produce results")

        # The ECS processor returns the event with the converted message field
        result_event = ecs_result.to_dict()
        ecs_record = json_parser(result_event['fields']['message'])

        # Verify ECS structure
        self._validate_ecs_structure(ecs_record)

        # Verify IP address conversion
        self._validate_ip_addresses(ecs_record, '192.168.1.1', '10.0.0.1')

    def _validate_ecs_structure(self, ecs_record: dict) -> None:
        """Helper method to validate ECS record structure."""
        required_fields = ['event', 'source', 'destination', 'network']

        for field in required_fields:
            self.assertIn(field, ecs_record, f"ECS record should contain {field}")

        # Check event metadata
        event_metadata = ecs_record['event']
        self.assertEqual(event_metadata['kind'], 'event')
        self.assertIn('network', event_metadata['category'])

    def _validate_ip_addresses(self, ecs_record: dict, expected_source: str,
                               expected_dest: str) -> None:
        """Helper method to validate IP address conversion."""
        # The ECS processor converts IPs to dotted decimal format
        if 'source' in ecs_record and 'ip' in ecs_record['source']:
            source_ip = ecs_record['source']['ip']
            self.assertEqual(source_ip, expected_source)

        if 'destination' in ecs_record and 'ip' in ecs_record['destination']:
            dest_ip = ecs_record['destination']['ip']
            self.assertEqual(dest_ip, expected_dest)

    def test_simple_ipfix_ecs_pipeline(self):
        """Test a simplified IPFIX to ECS pipeline."""
        # Create sample IPFIX-like data (simplified for testing)
        sample_ipfix_record = {
            'sourceIPv4Address': '192.168.1.100',
            'destinationIPv4Address': '10.0.0.50',
            'sourceTransportPort': 443,
            'destinationTransportPort': 80,
            'protocolIdentifier': 6,
            'octetDeltaCount': 1024,
            '@timestamp': 1640995200
        }

        # Process through ECS processor (wrap in expected format)
        wrapped_event = {
            "fields": {
                "message": sample_ipfix_record
            }
        }
        ecs_result = self.ecs_processor.process(wrapped_event)

        self.assertFalse(ecs_result.is_empty, "ECS processor should produce results")

        # The ECS processor returns the event with the converted message field
        result_event = ecs_result.to_dict()
        ecs_record = json_parser(result_event['fields']['message'])

        # Verify ECS structure using helper method
        self._validate_ecs_structure(ecs_record)

        # Verify IP addresses using helper method
        self._validate_ip_addresses(ecs_record, '192.168.1.100', '10.0.0.50')

    def test_ipfix_parser_integration(self):
        """Test that IPFIX parser works correctly for integration testing."""
        # Create a simple IPFIX message
        ipfix_stream = self.create_simple_ipfix_stream()

        # Parse using the IPFIX parser directly
        results = list(parse_ipfix_stream(ipfix_stream))

        self.assertGreater(len(results), 0, "Should produce results from IPFIX parsing")

        # Get the first parsed record
        record, _, _ = results[0]

        # Verify it's parsed IPFIX data
        self.assertIn('sourceIPv4Address', record)
        self.assertIn('destinationIPv4Address', record)
        self.assertIn('header', record)

        # Verify the IP addresses are correct
        self.assertEqual(record['sourceIPv4Address'], '192.168.1.1')
        self.assertEqual(record['destinationIPv4Address'], '10.0.0.1')

    def test_ecs_processor_registration(self):
        """Test that the ECS processor is registered correctly."""
        from processors.registry import ProcessorRegistry

        # The processor should be registered
        processor_class = ProcessorRegistry.get("ipfix_ecs")
        self.assertEqual(processor_class, ECSProcessor)

        # Should be able to create an instance
        processor = processor_class()
        self.assertIsInstance(processor, ECSProcessor)

    def test_end_to_end_ipfix_ecs_integration(self):
        """Test complete end-to-end IPFIX to ECS integration."""
        # Create multiple IPFIX records for more comprehensive testing
        test_flows = [
            ('192.168.1.100', '10.0.0.50'),
            ('172.16.0.10', '8.8.8.8'),
            ('10.0.1.100', '172.217.16.142')
        ]

        for source_ip, dest_ip in test_flows:
            with self.subTest(source=source_ip, destination=dest_ip):
                # Create IPFIX record (using dotted decimal format as parser outputs)
                ipfix_record = {
                    'sourceIPv4Address': source_ip,
                    'destinationIPv4Address': dest_ip,
                    'sourceTransportPort': '01bb',  # 443
                    'destinationTransportPort': '0050',  # 80
                    'protocolIdentifier': '06',  # TCP
                    '@timestamp': self.test_export_time,
                    'header': {
                        'export_time': self.test_export_time,
                        'sequence_number': self.test_sequence_number
                    }
                }

                # Process through ECS (wrap in expected format)
                wrapped_event = {
                    "fields": {
                        "message": ipfix_record
                    }
                }
                ecs_result = self.ecs_processor.process(wrapped_event)

                # Validate result
                self.assertFalse(ecs_result.is_empty)
                result_event = ecs_result.to_dict()
                ecs_record = json_parser(result_event['fields']['message'])

                self._validate_ecs_structure(ecs_record)
                self._validate_ip_addresses(ecs_record, source_ip, dest_ip)

    def _ip_to_hex(self, ip_address: str) -> str:
        """Convert IP address to hex string format."""
        return IPv4Address(ip_address).packed.hex()


if __name__ == '__main__':
    unittest.main()
