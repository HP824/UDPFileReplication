#include <iostream>
#include <fstream>
#include <cstring>
#include <cstdlib>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>
#include <vector>
#include <map>
#include <memory>
#include <ctime>
#include <iterator>
#include <locale>
#include <sys/stat.h>

using namespace std;

// #define DEBUG 1
#ifdef DEBUG
#define dprintf printf
#else
#define dprintf(fmt, ...) \
	do { \
	} while(0)
#endif

constexpr uint16_t CMD_PUT   = 1;
constexpr uint16_t CMD_DATA  = 2;
constexpr uint16_t CMD_ACK   = 3;
constexpr uint16_t CMD_ERROR = 4;

constexpr int TIMEOUT_SEC = 60;

struct Packet {
    uint32_t sequence_number;
    std::vector<char> data;
};

string currentRFC3339Time() {
    std::time_t t_now = std::time({});
    char rfc_time[24];
    std::strftime(std::data(rfc_time), std::size(rfc_time),
                  "%FT%TZ", std::gmtime(&t_now));
	return rfc_time;
}

int setRecvTimeout(int sockfd, time_t seconds) {
	timeval timeout{};
	timeout.tv_sec = seconds;
	timeout.tv_usec = 0;
	
	if(setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) < 0) {
		perror("Error setting socket timeout");
		return -1;
	}

	return 0;
}

class SWPChunk {
	public:
		uint16_t retry_cnt;
		std::vector<char> chunk;
		ssize_t chunk_size;

		SWPChunk(int mtu, ssize_t sz) : retry_cnt(0), chunk(mtu), chunk_size(sz)  {
		}
};

class SWPQueue {
	public:	
		std::map<uint32_t, auto_ptr<SWPChunk>> _chunks;
	public:
		int size() {
			return _chunks.size();
		}
};

class SWPReader {
	private:
		int _mtu;
		int _winsz;
		std::string _filename;
		std::ifstream _input_file;
    	uint32_t _seq_num;
		bool _eof;
		SWPQueue *_queue;
	public:
		SWPReader(int mtu, int winsz, const char *filename, SWPQueue *queue);
		int open_file();
		int close_file();
		int read_chunks();
		int addEOFBlock();
};

SWPReader::SWPReader(
		int mtu, int winsz, const char *filename, SWPQueue *queue) : 
	    _mtu(mtu), _winsz(winsz), _filename(filename), _queue(queue) {
	_seq_num = 0;
	_eof = false;
}

int SWPReader::open_file() {
    // Open input file
	_input_file.open(_filename, std::ios::binary);
    if (!_input_file.is_open()) {
        std::cerr << "Error: Cannot open input file '" << _filename << "'." << std::endl;
        return -1;
    }
	return 0;
}

int SWPReader::close_file() {
	_input_file.close();
	return 0;
}

int SWPReader::read_chunks() {
	dprintf("in read_chunks()\n");
	uint16_t cmd = CMD_DATA;
	while((int)_queue->_chunks.size() < _winsz) {
		if(_eof)
			break;
		auto_ptr<SWPChunk> send_buffer(new SWPChunk(_mtu, 0));

        _input_file.read(send_buffer->chunk.data() + sizeof(uint16_t) + sizeof(uint32_t), 
						_mtu - sizeof(uint16_t) - sizeof(uint32_t));
        std::streamsize bytes_read = _input_file.gcount();

		if (bytes_read == 0) {
			_eof = true;
			dprintf("EOF reached\n");
			continue;
		}

		send_buffer->chunk_size = bytes_read + sizeof(uint16_t) + sizeof(uint32_t);

		dprintf("Bytes read: %ld\n", bytes_read);
        
		// Prepend command and sequence number to the packet
        memcpy(send_buffer->chunk.data(), &cmd, sizeof(uint16_t));
        memcpy(send_buffer->chunk.data() + sizeof(uint16_t), &_seq_num, sizeof(uint32_t));
		
		_queue->_chunks[_seq_num] = send_buffer;
		
		_seq_num++;
	}
	return 0;
}

int SWPReader::addEOFBlock() {
	uint16_t cmd = CMD_DATA;
	auto_ptr<SWPChunk> send_buffer(new SWPChunk(_mtu, 0));
	send_buffer->chunk_size = sizeof(uint16_t) + sizeof(uint32_t);
    memcpy(send_buffer->chunk.data(), &cmd, sizeof(uint16_t));
    memcpy(send_buffer->chunk.data() + sizeof(uint16_t), &_seq_num, sizeof(uint32_t));
	_queue->_chunks[_seq_num] = send_buffer;
	return 0;
}

class SWPSender {
	private:
		int _mtu;
		int _winsz;
		int _sockfd;
		sockaddr_in *_server_addr;
		SWPQueue *_queue;
	public:
		SWPSender(int mtu, int winsz, int sockfd, sockaddr_in *server_addr, SWPQueue *queue);
		int send_chunk_data();
		int recv_chunk_ack();
};

SWPSender::SWPSender(
		int mtu, int winsz, int sockfd, sockaddr_in *server_addr, SWPQueue *queue) : 
	    _mtu(mtu), _winsz(winsz), _sockfd(sockfd), _server_addr(server_addr), _queue(queue) {
}

int SWPSender::send_chunk_data() {
	dprintf("in send_chunk_data()\n");
	uint32_t basesn = 0;
	if(_queue->_chunks.begin() != _queue->_chunks.end()) {
		basesn = _queue->_chunks.begin()->first;
	}
	for(std::map<uint32_t, auto_ptr<SWPChunk>>::iterator it=_queue->_chunks.begin(); 
			it!=_queue->_chunks.end(); ++it) {
		auto_ptr<SWPChunk>& send_buffer = it->second;
		
		uint32_t nextsn = 0;
		std::map<uint32_t, auto_ptr<SWPChunk>>::iterator nextit = it;
		nextit++;
		if(nextit != _queue->_chunks.end()) {
			nextsn = nextit->first;
		}

		dprintf("send buffer length: %u\n", send_buffer->chunk_size);

		if(send_buffer->retry_cnt != 0) {
			cerr << "Packet loss detected" << endl;
		}

		std::cout << currentRFC3339Time() << ", DATA, " << it->first << ", " << basesn << ", " << nextsn << ", " << basesn + _winsz <<  endl;

		ssize_t bytes_sent = sendto(_sockfd, 
				                    send_buffer->chunk.data(), 
									send_buffer->chunk_size, 
									0, (struct sockaddr *)_server_addr, sizeof(*_server_addr));
        if (bytes_sent < 0) {
            perror("Error sending packet");
            return -1;
        }
		send_buffer->retry_cnt++;
	}
	return 0;
}

int SWPSender::recv_chunk_ack() {
	setRecvTimeout(_sockfd, 5);

	uint32_t basesn = 0;
	if(_queue->_chunks.begin() != _queue->_chunks.end()) {
		basesn = _queue->_chunks.begin()->first;
	}
	while(_queue->size() > 0) {
		dprintf("waiting for ack\n");
		std::vector<char> recv_buffer(_mtu);	
		ssize_t bytes_received = recvfrom(_sockfd, recv_buffer.data(), _mtu, 0,
                                          NULL, NULL);
        if (bytes_received < 0) {
            std::cerr << "Cannot detect server" << std::endl;
            break;
        }

		dprintf("Bytes recieved: %ld\n", bytes_received);

		uint16_t received_cmd;
		uint32_t received_seq_number;

		memcpy(&received_cmd, recv_buffer.data(), sizeof(uint16_t));
		memcpy(&received_seq_number, recv_buffer.data() + sizeof(uint16_t), sizeof(uint32_t));

		if(received_cmd == CMD_ERROR) {
			printf("Received Error. Error code is: %ud\n", received_seq_number);
			return -1;
		}

		if(received_cmd != CMD_ACK) {
			perror("Invalid ACK Packet\n");
			return -1;
		}

		std::map<uint32_t, auto_ptr<SWPChunk>>::iterator it = _queue->_chunks.find(received_seq_number);

		if(it == _queue->_chunks.end()) {
			perror("Invalid Sequence Number in ACK Packet\n");
		} else {
			uint32_t nextsn = 0;
			std::map<uint32_t, auto_ptr<SWPChunk>>::iterator nextit = it;
			nextit++;
			if(nextit != _queue->_chunks.end()) {
				nextsn = nextit->first;
			}
			cout << currentRFC3339Time() << ", ACK, " << it->first << ", " << basesn << ", " << nextsn << ", " << basesn + _winsz <<  endl;
			_queue->_chunks.erase(it);
		}
	}
	dprintf("received chunk ack complete\n");
	for(std::map<uint32_t, auto_ptr<SWPChunk>>::iterator it=_queue->_chunks.begin(); 
			it!=_queue->_chunks.end(); ++it) {
		// cout << "it->first: " << it->first << endl;
		auto_ptr<SWPChunk>& send_buffer = it->second;
		if(send_buffer->retry_cnt == 5) {
			cerr << "Reached max re-transmission limit" << endl;
			return -1;
		}
	}
	dprintf("retransmission check complete\n");
	return 0;
}

int sendPutPacket(int sockfd, sockaddr_in *server_addr, const char *filename) {
    std::vector<char> send_buffer(1024);
    std::vector<char> recv_buffer(1024);

	uint16_t cmd = CMD_PUT;

	ssize_t length = strlen(filename);

    memcpy(send_buffer.data(), &cmd, sizeof(uint16_t));
	memcpy(send_buffer.data() + sizeof(uint16_t), filename, length + 1);

	dprintf("Sending PUT packet...\n");

	ssize_t bytes_sent = sendto(sockfd, send_buffer.data(), length + sizeof(uint16_t), 0,
								(struct sockaddr *)server_addr, sizeof(*server_addr));
	if (bytes_sent < 0) {
		perror("Error sending packet\n");
		return -1;
	}

	ssize_t bytes_received = recvfrom(sockfd, recv_buffer.data(), 1024, 0,
									  NULL, NULL);
	if (bytes_received < 0) {
		dprintf("Bytes received: %ld", bytes_received);
		perror("Cannot detect server\n");
		return -1;
    }

	if(bytes_received < sizeof(uint16_t) + sizeof(uint32_t)) {
		perror("Unexpected Buffer Size\n");
		return -1;
	}
	
	uint32_t received_seq_number;
    memcpy(&cmd, recv_buffer.data(), sizeof(uint16_t));
	memcpy(&received_seq_number, recv_buffer.data() + sizeof(uint16_t), sizeof(uint32_t));
	
	if(cmd == CMD_ERROR) {
		printf("Received Error. Error code is: %ud\n", received_seq_number);
		return -1;
	}

	if(cmd != CMD_ACK) {
		perror("Invalid ACK Packet\n");
		return -1;
	}

	if(received_seq_number != 0) {
		perror("Invalid Sequence Number in ACK Packet\n");
		return -1;
	}
	
	dprintf("Successful PUT packet.\n");
	return 0;
}

int checkFile(const char* file_path) {
 	struct stat sb;

    if (stat(file_path, &sb) != 0) {
        dprintf("File Does Not Exist: %s\n", file_path);
		return -1;
    } else {
        if (!S_ISREG(sb.st_mode)) {
            dprintf("Path is not a file: %s\n", file_path);
            return -1;
        }
    }
	return 0;
}

int main(int argc, char *argv[]) {
    if (argc != 7) {
        std::cerr << "Usage: ./myclient <server_ip> <server_port> <mtu> <winsz> <in_file> <out_file>" << std::endl;
        return EXIT_FAILURE;
    }

    std::string server_ip = argv[1];
    int server_port = std::stoi(argv[2]);
    int mtu = std::stoi(argv[3]);
    int winsz = std::stoi(argv[4]);
    std::string input_file_path = argv[5];
	std::string output_file_path = argv[6];

    if (mtu <= sizeof(uint16_t) + sizeof(uint32_t)) {
        std::cerr << "Required minimum MTU is " << sizeof(uint16_t) + sizeof(uint32_t) + 1 << std::endl;
        return EXIT_FAILURE;
    }
	
	if(checkFile(input_file_path.c_str())) {
		std::cerr << "Invalid Input File" << std::endl;
		return EXIT_FAILURE;
	}

    int client_socket = socket(AF_INET, SOCK_DGRAM, 0);
    if (client_socket < 0) {
        perror("Error creating socket");
        return EXIT_FAILURE;
    }

    sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(server_port);

    if (inet_pton(AF_INET, server_ip.c_str(), &server_addr.sin_addr) <= 0) {
        perror("Error with server IP");
        close(client_socket);
        return EXIT_FAILURE;
    }
	 
    // Set timeout for receiving packets
    timeval timeout{};
    timeout.tv_sec = TIMEOUT_SEC;
    timeout.tv_usec = 0;
    if (setsockopt(client_socket, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) < 0) {
        perror("Error setting socket timeout");
        close(client_socket);
        return EXIT_FAILURE;
    }

	// Send PUT packet
	int ret_val = sendPutPacket(client_socket, &server_addr, output_file_path.c_str());

	if(ret_val < 0) {
        close(client_socket);
        return EXIT_FAILURE;	
	}

	SWPQueue queue;
	SWPReader swp_reader(mtu, winsz, input_file_path.c_str(), &queue);
	SWPSender swp_sender(mtu, winsz, client_socket, &server_addr, &queue);

	ret_val = swp_reader.open_file();
	if(ret_val < 0) {
		std::cerr << "Error: Cannot open input file '" << input_file_path << "'." << std::endl;
        close(client_socket);
        return EXIT_FAILURE;
	}
	
	bool eof = false;
	while(1) {
		ret_val = swp_reader.read_chunks();
		dprintf("read chunks\n");
		if(ret_val < 0) {
			std::cerr << "read chunk data failure" << std::endl;
			close(client_socket);
			return EXIT_FAILURE;		
		}

		if(eof == false && queue.size() == 0) {
			swp_reader.addEOFBlock();
			eof = true;
		}

		if(eof == true && queue.size() == 0) {
			break;
		}

		ret_val = swp_sender.send_chunk_data();
		if(ret_val < 0) {
			std::cerr << "send chunk data failure" << std::endl;
        	close(client_socket);
        	return EXIT_FAILURE;
		}
		
		ret_val = swp_sender.recv_chunk_ack();
		if(ret_val < 0) {
			// cerr << "receive ACK failure" << std::endl;
        	close(client_socket);
        	return EXIT_FAILURE;
		}
	}

	swp_reader.close_file();

    close(client_socket);

    std::cout << "File transfer completed successfully!" << std::endl;
    return EXIT_SUCCESS;
}

