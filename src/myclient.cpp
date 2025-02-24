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
#include <sys/time.h>
#include <pthread.h>

using namespace std;

#define handle_error_en(en, msg) \
	   do { errno = en; perror(msg); exit(EXIT_FAILURE); } while (0)

#define handle_error(msg) \
	   do { perror(msg); exit(EXIT_FAILURE); } while (0)

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
    time_t t_now = time({});
    char rfc_time[24];
    strftime(data(rfc_time), size(rfc_time),
                  "%FT%TZ", gmtime(&t_now));
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
		vector<uint16_t> retry_cnt;
		vector<char> chunk;
		ssize_t chunk_size;
		vector<bool> recv_ack_bits;

		SWPChunk(int mtu, ssize_t sz, int num_senders) : retry_cnt(num_senders, 0), chunk(mtu), chunk_size(sz), recv_ack_bits(num_senders, false)  {
		}
		void setAckBit(int sender_id, bool flag) {
			recv_ack_bits[sender_id] = flag;
		}
		void incRetryCount(int sender_id) {
			retry_cnt[sender_id] = retry_cnt[sender_id] + 1;
		}
		uint16_t getRetryCount(int sender_id) {
			return retry_cnt[sender_id];
		}
		bool isChunkSent() {
			for(ssize_t i = 0; i < (ssize_t)recv_ack_bits.size(); i++) {
				if(recv_ack_bits[i] == false)
					return false;
			}
			return true;
		}

		bool isAckSet(int sender_id) {
			return recv_ack_bits[sender_id];
		}
};

class SWPQueue {
	public:	
		std::map<uint32_t, shared_ptr<SWPChunk>> _chunks;
		pthread_cond_t reader_cond;
		pthread_cond_t sender_cond;
		pthread_mutex_t queue_mutex;
		bool _eof;
		int _sender_count;
		int _senders;

	public:
		SWPQueue(int sender_count);
		int size() {
			return _chunks.size();
		}
		int size2(int sender_id) {
			int count = 0;
			map<uint32_t, shared_ptr<SWPChunk>>::iterator it;
			for(it = _chunks.begin(); it != _chunks.end(); it++) {
				if(!it->second->isAckSet(sender_id))
					count++;
			}
			dprintf("Queue Size for Sender %d is %d\n", sender_id, count);
			return count;
		}
		void setEOF(bool eof) {
			_eof = eof;
		}
		bool isEOF() {
			return _eof;
		}
		int deleteSentChunks() {
			dprintf("deleting sent chunks\n");
			map<uint32_t, shared_ptr<SWPChunk>>::iterator it;
			for(it = _chunks.begin(); it != _chunks.end();) {
				map<uint32_t, shared_ptr<SWPChunk>>::iterator it2 = it;
				it++;
				// dprintf("checking chunk %u\n", it2->first);
				if(it2->second->isChunkSent()) {
					_chunks.erase(it2);
					// dprintf("  deleting chunk\n");  
				}
			}
			/* 
			dprintf("received chunk ack complete\n");
			for(it=_chunks.begin(); it!=_chunks.end(); ++it) {
				cout << "it->first: " << it->first << endl;
				shared_ptr<SWPChunk> send_buffer = it->second;
				if(send_buffer->getRetryCount() == 5) {
					cerr << "Reached max re-transmission limit" << endl;
					return -1;
				}
			}
			dprintf("retransmission check complete\n");
			*/
			return 0;
		}
};

SWPQueue::SWPQueue(int sender_count) {
	pthread_cond_init(&reader_cond, NULL);
	pthread_cond_init(&sender_cond, NULL);
	pthread_mutex_init(&queue_mutex, NULL);
	_eof = false;
	_sender_count = sender_count;
	_senders = 0;
}

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
		shared_ptr<SWPChunk> send_buffer(new SWPChunk(_mtu, 0, _queue->_sender_count));

        _input_file.read(send_buffer->chunk.data() + sizeof(uint16_t) + sizeof(uint32_t), 
						_mtu - sizeof(uint16_t) - sizeof(uint32_t));
        std::streamsize bytes_read = _input_file.gcount();

		if (bytes_read == 0) {
			_eof = true;
			dprintf("EOF reached\n");
			continue;
		}

		send_buffer->chunk_size = bytes_read + sizeof(uint16_t) + sizeof(uint32_t);

		// dprintf("Bytes read: %ld\n", bytes_read);
        
		// Prepend command and sequence number to the packet
        memcpy(send_buffer->chunk.data(), &cmd, sizeof(uint16_t));
        memcpy(send_buffer->chunk.data() + sizeof(uint16_t), &_seq_num, sizeof(uint32_t));
		
		_queue->_chunks[_seq_num] = send_buffer;
		
		_seq_num++;
	}
	return 0;
}

int SWPReader::addEOFBlock() {
	dprintf("adding EOF Block\n");
	uint16_t cmd = CMD_DATA;
	shared_ptr<SWPChunk> send_buffer(new SWPChunk(_mtu, 0, _queue->_sender_count));
	send_buffer->chunk_size = sizeof(uint16_t) + sizeof(uint32_t);
    memcpy(send_buffer->chunk.data(), &cmd, sizeof(uint16_t));
    memcpy(send_buffer->chunk.data() + sizeof(uint16_t), &_seq_num, sizeof(uint32_t));
	_queue->_chunks[_seq_num] = send_buffer;
	return 0;
}

class SWPSender {
	private:
		int _sender_id;
		int _mtu;
		int _winsz;
		int _sockfd;
		sockaddr_in *_server_addr;
		SWPQueue *_queue;
	public:
		int chunks_sent;
	public:
		SWPSender(int sender_id, int mtu, int winsz, int sockfd, sockaddr_in *server_addr, SWPQueue *queue);
		int send_chunk_data();
		int recv_chunk_ack();
};

SWPSender::SWPSender(
		int sender_id, int mtu, int winsz, int sockfd, sockaddr_in *server_addr, SWPQueue *queue) : 
	    _sender_id(sender_id), _mtu(mtu), _winsz(winsz), _sockfd(sockfd), _server_addr(server_addr), _queue(queue) {
}

int SWPSender::send_chunk_data() {
	dprintf("in send_chunk_data()\n");
	uint32_t basesn = 0;
	if(_queue->_chunks.begin() != _queue->_chunks.end()) {
		basesn = _queue->_chunks.begin()->first;
	}
	chunks_sent = 0;

	for(std::map<uint32_t, shared_ptr<SWPChunk>>::iterator it=_queue->_chunks.begin(); 
			it!=_queue->_chunks.end(); ++it) {
		shared_ptr<SWPChunk> send_buffer = it->second;
		if(it->second->isAckSet(_sender_id))
			continue;
		chunks_sent++;
		uint32_t nextsn = 0;
		std::map<uint32_t, shared_ptr<SWPChunk>>::iterator nextit = it;
		nextit++;
		if(nextit != _queue->_chunks.end()) {
			nextsn = nextit->first;
		}

		// dprintf("send buffer length: %lu\n", send_buffer->chunk_size);

		if(send_buffer->getRetryCount(_sender_id) != 0) {
			cerr << "Packet loss detected" << endl;
		}
		if(send_buffer->getRetryCount(_sender_id) == 5) {
			it->second->setAckBit(_sender_id, true);
			return -1;
		}

		std::cout << currentRFC3339Time() << ", DATA, " << it->first << ", " << basesn << ", " << nextsn << ", " << basesn + _winsz <<  endl;

		ssize_t bytes_sent = sendto(_sockfd, 
				                    send_buffer->chunk.data(), 
									send_buffer->chunk_size, 
									0, (struct sockaddr *)_server_addr, sizeof(*_server_addr));
        if (bytes_sent < 0) {
            perror("Error sending packet");
            return -2;
        }
		send_buffer->incRetryCount(_sender_id);
	}
	return 0;
}

int SWPSender::recv_chunk_ack() {
	setRecvTimeout(_sockfd, 5);

	uint32_t basesn = 0;
	if(_queue->_chunks.begin() != _queue->_chunks.end()) {
		basesn = _queue->_chunks.begin()->first;
	}

	int queue_size = chunks_sent;

	while(queue_size > 0) {
		// dprintf("waiting for ack\n");
		std::vector<char> recv_buffer(_mtu);	
		ssize_t bytes_received = recvfrom(_sockfd, recv_buffer.data(), _mtu, 0,
                                          NULL, NULL);
        if (bytes_received < 0)
			return -1;

		// dprintf("Bytes received: %ld\n", bytes_received);

		uint16_t received_cmd;
		uint32_t received_seq_number;

		memcpy(&received_cmd, recv_buffer.data(), sizeof(uint16_t));
		memcpy(&received_seq_number, recv_buffer.data() + sizeof(uint16_t), sizeof(uint32_t));

		if(received_cmd == CMD_ERROR) {
			printf("Received Error. Error code is: %ud\n", received_seq_number);
			return -2;
		}

		if(received_cmd != CMD_ACK) {
			perror("Invalid ACK Packet\n");
			return -3;
		}

		std::map<uint32_t, shared_ptr<SWPChunk>>::iterator it = _queue->_chunks.find(received_seq_number);

		if(it == _queue->_chunks.end()) {
			perror("Invalid Sequence Number in ACK Packet\n");
		} else {
			uint32_t nextsn = 0;
			std::map<uint32_t, shared_ptr<SWPChunk>>::iterator nextit = it;
			nextit++;
			if(nextit != _queue->_chunks.end()) {
				nextsn = nextit->first;
			}
			cout << currentRFC3339Time() << ", ACK, " << it->first << ", " << basesn << ", " << nextsn << ", " << basesn + _winsz <<  endl;
			queue_size--;
			it->second->setAckBit(_sender_id, true);
		}
	}
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
		return -2;
    }

	if(bytes_received < (ssize_t)(sizeof(uint16_t) + sizeof(uint32_t))) {
		perror("Unexpected Buffer Size\n");
		return -3;
	}
	
	uint32_t received_seq_number;
    memcpy(&cmd, recv_buffer.data(), sizeof(uint16_t));
	memcpy(&received_seq_number, recv_buffer.data() + sizeof(uint16_t), sizeof(uint32_t));
	
	if(cmd == CMD_ERROR) {
		printf("Received Error. Error code is: %ud\n", received_seq_number);
		return -4;
	}

	if(cmd != CMD_ACK) {
		perror("Invalid ACK Packet\n");
		return -5;
	}

	if(received_seq_number != 0) {
		perror("Invalid Sequence Number in ACK Packet\n");
		return -6;
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

typedef struct reader_param_t {
	int mtu;
	int winsz;
	std::string infile_path;
	SWPQueue *queue;
} reader_param;

typedef struct sender_param_t {
	int sender_id;
	pthread_t sender_tid;
	int mtu;
	int winsz;
	std::string server_ip;
	int server_port;
	std::string outfile_path;
	SWPQueue *queue;
} sender_param;

static void *
reader_thread(void *arg) {
	reader_param *param = (reader_param *)arg;
	SWPReader swp_reader(param->mtu, param->winsz, param->infile_path.c_str(), param->queue);
	sleep(1);
	int ret_val = swp_reader.open_file();
	if(ret_val < 0) {
		cerr << "Error: Cannot open input file '" << param->infile_path << "'." << endl;
        return NULL;
	}
	
	bool eof = false;
	while(1) {
		dprintf("R: reading chunks\n");
		pthread_mutex_lock(&(param->queue->queue_mutex));
		ret_val = swp_reader.read_chunks();
		dprintf("R: read chunks complete\n");

		if(ret_val < 0) {
			pthread_mutex_unlock(&(param->queue->queue_mutex));
			std::cerr << "R: read chunk data failure" << std::endl;
			return NULL;		
		}

		if(eof == false && param->queue->size() == 0) {
			swp_reader.addEOFBlock();
			eof = true;
			param->queue->setEOF(eof);
		}

		dprintf("R: signalling sender, size: %d\n", param->queue->size());
		pthread_cond_signal(&(param->queue->sender_cond));
		pthread_mutex_unlock(&(param->queue->queue_mutex));

		if(eof == true && param->queue->size() == 0) {
			dprintf("R: EOF reached, queue size is 0, breaking loop\n");
			break;
		}

		dprintf("R: waiting on reader cond\n");
		
		// delete all sent chunks
		pthread_mutex_lock(&(param->queue->queue_mutex));
		dprintf("R: acquired lock\n");
		struct timespec timeout;
		struct timeval now;

		gettimeofday(&now, NULL);
		timeout.tv_sec = now.tv_sec+2;
		timeout.tv_nsec = 1000*(now.tv_usec);
		do {
			// pthread_cond_wait(&(param->queue->reader_cond), &(param->queue->queue_mutex));
			dprintf("R: number of senders is %d\n", param->queue->_senders);
			pthread_cond_timedwait(&(param->queue->reader_cond), &(param->queue->queue_mutex), &timeout);
		} while(param->queue->_senders > 0);
		param->queue->deleteSentChunks();
		pthread_mutex_unlock(&(param->queue->queue_mutex));

		dprintf("R: reader cond signalled\n");
	}

	swp_reader.close_file();
	dprintf("R: reader thread completed\n");
	return NULL;
}

static void *
sender_thread(void *arg) {
	sender_param *param = (sender_param *)arg;
	dprintf("creating socket\n");
    int client_socket = socket(AF_INET, SOCK_DGRAM, 0);
    if (client_socket < 0) {
        perror("Error creating socket");
        return NULL;
    }

    sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(param->server_port);

    if (inet_pton(AF_INET, param->server_ip.c_str(), &server_addr.sin_addr) <= 0) {
        perror("Error with server IP");
        close(client_socket);
        return NULL;
    }
	 
    // Set timeout for receiving packets
    timeval timeout{};
    timeout.tv_sec = TIMEOUT_SEC;
    timeout.tv_usec = 0;
    if (setsockopt(client_socket, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) < 0) {
        perror("Error setting socket timeout");
        close(client_socket);
        return NULL;
    }

	// Send PUT packet
	dprintf("S: sending put packet\n");
	int ret_val = sendPutPacket(client_socket, &server_addr, param->outfile_path.c_str());

	if(ret_val < 0) {
		if(ret_val >= -2) {
			cerr << "Server is down IP " << param->server_ip << " port " 
				<< param->server_port << endl;
			exit(5);
		}
        close(client_socket);
        return NULL;	
	}

	SWPSender swp_sender(param->sender_id, param->mtu, param->winsz, client_socket, &server_addr, param->queue);
	while(1) {
		dprintf("S: checking queue\n");
		pthread_mutex_lock(&(param->queue->queue_mutex));
		dprintf("S: acquired queue lock\n");
	    while(param->queue->size2(param->sender_id) == 0 && !param->queue->isEOF())
			pthread_cond_wait(&(param->queue->sender_cond), &(param->queue->queue_mutex));
		
		int queue_size = param->queue->size2(param->sender_id);
		if(!(queue_size == 0 && param->queue->isEOF()))
			param->queue->_senders++;

		dprintf("S: sender cond signalled\n");
		pthread_mutex_unlock(&(param->queue->queue_mutex));
		dprintf("S: processing chunk\n");

		if(queue_size == 0 && param->queue->isEOF())
			break;
		ret_val = swp_sender.send_chunk_data();
		if(ret_val < 0) {
			cerr << "Reached max re-transmission limit IP " << param->server_ip.c_str() 
				<< endl;
			exit(4);
        	// return NULL;
		}

		if(queue_size == 0 && param->queue->isEOF())
			break;
		if(swp_sender.chunks_sent == 0) {
			usleep(100000);
			continue;
		}
		
		ret_val = swp_sender.recv_chunk_ack();
		if(ret_val < 0) {
			if(ret_val == -1) {
				cerr << "Cannot detect server IP " << param->server_ip.c_str() << " port "
					<< param->server_port << endl;
				exit(3);
			}
			dprintf("receive ACK failure");
        	close(client_socket);
        	return NULL;
		}
		dprintf("S: signalling reader\n");

		pthread_mutex_lock(&(param->queue->queue_mutex));
		param->queue->_senders--;
		pthread_cond_signal(&(param->queue->reader_cond));
		pthread_mutex_unlock(&(param->queue->queue_mutex));
		usleep(10000);
	}
	close(client_socket);
	dprintf("S: sender completed successfully\n");
	return NULL;
}

int readServerConfig(string config_file, vector<pair<string, int>> &arr) {
	ifstream ifs (config_file, ifstream::in);
	
	while(ifs.good() && !ifs.eof()) {
		char line[256];
		ifs.getline(line, 256);
		if(strlen(line) == 0)
			continue;
		
		if(line[0] == '#')
			continue;

		char *ip = strtok(line, " ");
		char *port = strtok(NULL, " ");

		if(ip == NULL || port == NULL) {
			return -1;
		}
		dprintf("IP: %s, Port: %s\n", ip, port);
		arr.push_back(make_pair(string(ip), stoi(port)));
	}
	ifs.close();
	return 0;
}

int main(int argc, char *argv[]) {
    if (argc != 7) {
        cerr << "Usage: ./myclient <servn> <servaddr.conf> <mss> <winsz> <in_file_path> <out_file_path>" << endl;
        return EXIT_FAILURE;
    }

    int servn = stoi(argv[1]);
	std::string serv_conf = argv[2];
    int mtu = std::stoi(argv[3]);
    int winsz = std::stoi(argv[4]);
    std::string input_file_path = argv[5];
	std::string output_file_path = argv[6];

    if(mtu <= (int)(sizeof(uint16_t) + sizeof(uint32_t))) {
        std::cerr << "Required minimum MSS is " << sizeof(uint16_t) + sizeof(uint32_t) + 1 << std::endl;
        return EXIT_FAILURE;
    }
	
	if(checkFile(input_file_path.c_str())) {
		std::cerr << "Invalid Input File" << std::endl;
		return EXIT_FAILURE;
	}
	
	vector<pair<string, int>> servers;
	int ret = readServerConfig(serv_conf, servers);
	if(ret == -1) {
		cerr << "Malformed config file!" << endl;
		exit(6);
	}

	int sender_count = servn;
	if(sender_count > (int)servers.size()) {
		cerr << "Calling more servers than provided in your config file!" << endl;
		return EXIT_FAILURE;
	}
	SWPQueue queue(sender_count);
	reader_param r_param = {mtu, winsz, input_file_path, &queue};

	pthread_attr_t attr;

	int s = pthread_attr_init(&attr);
	if (s != 0)
	   handle_error_en(s, "pthread_attr_init");

	pthread_t tid1;
	s = pthread_create(&tid1, &attr, &reader_thread, &r_param);
	if (s != 0)
	   handle_error_en(s, "pthread_create for reader");

	sender_param *s_param;
	s_param = (sender_param *)calloc(sender_count, sizeof(sender_param));

	for(int i = 0; i < sender_count; i++) {
		pair<string, int> server = servers.at(i);

		s_param[i].sender_id = i;
		s_param[i].mtu = mtu;
		s_param[i].winsz = winsz;
		s_param[i].server_ip = server.first;
		s_param[i].server_port = server.second;
		s_param[i].outfile_path = output_file_path;
		s_param[i].queue = &queue;

		s = pthread_create(&s_param[i].sender_tid, &attr, &sender_thread, &s_param[i]);
		if (s != 0)
		   handle_error_en(s, "pthread_create for sender");
	}

	s = pthread_attr_destroy(&attr);
	if (s != 0)
	   handle_error_en(s, "pthread_attr_destroy");

	void *res;
	s = pthread_join(tid1, &res);
	if (s != 0)
		handle_error_en(s, "pthread_join for reader");

	for(int i = 0; i < sender_count; i++) {
		s = pthread_join(s_param[i].sender_tid, &res);
		if (s != 0)
			handle_error_en(s, "pthread_join for sender");
	}
	
    cout << "File transfer completed successfully!" << endl;
    return EXIT_SUCCESS;
}

