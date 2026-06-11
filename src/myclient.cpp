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
#include <sys/stat.h>
#include <sys/time.h>
#include <pthread.h>
#include <atomic>
#include <iomanip>
#include <sstream>

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
constexpr int MAX_RETRIES = 5;

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

// One in-flight DATA packet tracked per replica server.
class GBNPacket {
	public:
		vector<uint16_t> retry_cnt;
		vector<char> chunk;
		ssize_t chunk_size;
		vector<bool> acked;

		GBNPacket(int mtu, ssize_t sz, int num_senders)
			: retry_cnt(num_senders, 0), chunk(mtu), chunk_size(sz),
			  acked(num_senders, false) {}

		void setAcked(int sender_id, bool flag) {
			acked[sender_id] = flag;
		}
		void incRetryCount(int sender_id) {
			retry_cnt[sender_id]++;
		}
		uint16_t getRetryCount(int sender_id) const {
			return retry_cnt[sender_id];
		}
		bool isAckedByAll() const {
			for (bool a : acked) {
				if (!a)
					return false;
			}
			return true;
		}
		bool isAcked(int sender_id) const {
			return acked[sender_id];
		}
};

// Shared send window for all replica senders (Go-Back-N).
class GBNWindow {
	public:
		map<uint32_t, shared_ptr<GBNPacket>> _packets;
		pthread_cond_t reader_cond;
		pthread_cond_t sender_cond;
		pthread_mutex_t queue_mutex;
		bool _eof;
		int _sender_count;
		int _senders;
		atomic<uint64_t> _retransmits;

		GBNWindow(int sender_count);
		uint64_t retransmits() const { return _retransmits.load(); }
		int inFlightCount() const {
			return _packets.size();
		}
		int unackedCount(int sender_id) const {
			int count = 0;
			for (const auto &entry : _packets) {
				if (!entry.second->isAcked(sender_id))
					count++;
			}
			dprintf("Unacked packets for sender %d: %d\n", sender_id, count);
			return count;
		}
		// Smallest sequence number not yet ACKed by this sender (send base).
		uint32_t baseSeq(int sender_id) const {
			for (const auto &entry : _packets) {
				if (!entry.second->isAcked(sender_id))
					return entry.first;
			}
			return 0;
		}
		void setEOF(bool eof) {
			_eof = eof;
		}
		bool isEOF() const {
			return _eof;
		}
		void deleteFullyAcked() {
			dprintf("deleting fully acked packets\n");
			for (auto it = _packets.begin(); it != _packets.end();) {
				auto current = it++;
				if (current->second->isAckedByAll())
					_packets.erase(current);
			}
		}
};

GBNWindow::GBNWindow(int sender_count) {
	pthread_cond_init(&reader_cond, NULL);
	pthread_cond_init(&sender_cond, NULL);
	pthread_mutex_init(&queue_mutex, NULL);
	_eof = false;
	_sender_count = sender_count;
	_senders = 0;
	_retransmits = 0;
}

class GBNReader {
	private:
		int _mtu;
		int _winsz;
		string _filename;
		ifstream _input_file;
		uint32_t _next_seq;
		bool _file_eof;
		GBNWindow *_window;
	public:
		GBNReader(int mtu, int winsz, const char *filename, GBNWindow *window);
		int open_file();
		int close_file();
		int fillWindow();
		int addEOFPacket();
};

GBNReader::GBNReader(int mtu, int winsz, const char *filename, GBNWindow *window)
	: _mtu(mtu), _winsz(winsz), _filename(filename), _window(window) {
	_next_seq = 0;
	_file_eof = false;
}

int GBNReader::open_file() {
	_input_file.open(_filename, ios::binary);
	if (!_input_file.is_open()) {
		cerr << "Error: Cannot open input file '" << _filename << "'." << endl;
		return -1;
	}
	return 0;
}

int GBNReader::close_file() {
	_input_file.close();
	return 0;
}

int GBNReader::fillWindow() {
	dprintf("in fillWindow()\n");
	uint16_t cmd = CMD_DATA;
	while ((int)_window->_packets.size() < _winsz) {
		if (_file_eof)
			break;

		shared_ptr<GBNPacket> pkt(
			new GBNPacket(_mtu, 0, _window->_sender_count));

		_input_file.read(
			pkt->chunk.data() + sizeof(uint16_t) + sizeof(uint32_t),
			_mtu - sizeof(uint16_t) - sizeof(uint32_t));
		streamsize bytes_read = _input_file.gcount();

		if (bytes_read == 0) {
			_file_eof = true;
			dprintf("file EOF reached\n");
			continue;
		}

		pkt->chunk_size = bytes_read + sizeof(uint16_t) + sizeof(uint32_t);
		memcpy(pkt->chunk.data(), &cmd, sizeof(uint16_t));
		memcpy(pkt->chunk.data() + sizeof(uint16_t), &_next_seq, sizeof(uint32_t));

		_window->_packets[_next_seq] = pkt;
		_next_seq++;
	}
	return 0;
}

int GBNReader::addEOFPacket() {
	dprintf("adding EOF packet\n");
	uint16_t cmd = CMD_DATA;
	shared_ptr<GBNPacket> pkt(
		new GBNPacket(_mtu, 0, _window->_sender_count));
	pkt->chunk_size = sizeof(uint16_t) + sizeof(uint32_t);
	memcpy(pkt->chunk.data(), &cmd, sizeof(uint16_t));
	memcpy(pkt->chunk.data() + sizeof(uint16_t), &_next_seq, sizeof(uint32_t));
	_window->_packets[_next_seq] = pkt;
	return 0;
}

class GBNSender {
	private:
		int _sender_id;
		int _winsz;
		int _sockfd;
		sockaddr_in *_server_addr;
		GBNWindow *_window;
		int _packets_in_flight;
	public:
		GBNSender(int sender_id, int winsz, int sockfd,
		          sockaddr_in *server_addr, GBNWindow *window);
		int packetsInFlight() const { return _packets_in_flight; }
		// Send (or retransmit) every unacked packet in [base, base + winsz).
		int sendWindow();
		int recvAcks();
};

GBNSender::GBNSender(int sender_id, int winsz, int sockfd,
                     sockaddr_in *server_addr, GBNWindow *window)
	: _sender_id(sender_id), _winsz(winsz), _sockfd(sockfd),
	  _server_addr(server_addr), _window(window), _packets_in_flight(0) {}

int GBNSender::sendWindow() {
	dprintf("in sendWindow()\n");
	_packets_in_flight = 0;

	if (_window->_packets.empty())
		return 0;

	uint32_t base = _window->baseSeq(_sender_id);
	uint32_t window_end = base + _winsz;

	for (auto it = _window->_packets.begin(); it != _window->_packets.end(); ++it) {
		if (it->first >= window_end)
			break;
		if (it->second->isAcked(_sender_id))
			continue;

		_packets_in_flight++;

		if (it->second->getRetryCount(_sender_id) != 0) {
			cerr << "Packet loss detected" << endl;
			_window->_retransmits++;
		}
		if (it->second->getRetryCount(_sender_id) == MAX_RETRIES) {
			it->second->setAcked(_sender_id, true);
			return -1;
		}

		cout << currentRFC3339Time() << ", DATA, " << it->first
		     << ", " << base << ", " << window_end << endl;

		ssize_t bytes_sent = sendto(_sockfd, it->second->chunk.data(),
		                            it->second->chunk_size, 0,
		                            (struct sockaddr *)_server_addr,
		                            sizeof(*_server_addr));
		if (bytes_sent < 0) {
			perror("Error sending packet");
			return -2;
		}
		it->second->incRetryCount(_sender_id);
	}
	return 0;
}

int GBNSender::recvAcks() {
	setRecvTimeout(_sockfd, 5);

	uint32_t base = _window->_packets.empty() ? 0 : _window->baseSeq(_sender_id);
	uint32_t window_end = base + _winsz;
	int acks_remaining = _packets_in_flight;

	while (acks_remaining > 0) {
		vector<char> recv_buffer(4096);
		ssize_t bytes_received = recvfrom(_sockfd, recv_buffer.data(),
		                                  recv_buffer.size(), 0, NULL, NULL);
		if (bytes_received < 0)
			return -1;

		uint16_t received_cmd;
		uint32_t received_seq;

		memcpy(&received_cmd, recv_buffer.data(), sizeof(uint16_t));
		memcpy(&received_seq, recv_buffer.data() + sizeof(uint16_t),
		       sizeof(uint32_t));

		if (received_cmd == CMD_ERROR) {
			printf("Received Error. Error code is: %ud\n", received_seq);
			return -2;
		}
		if (received_cmd != CMD_ACK) {
			cerr << "Invalid ACK packet" << endl;
			return -3;
		}

		auto it = _window->_packets.find(received_seq);
		if (it == _window->_packets.end()) {
			cerr << "ACK for unknown sequence number: " << received_seq << endl;
			continue;
		}
		if (received_seq < base || received_seq >= window_end) {
			cerr << "ACK outside send window: " << received_seq << endl;
			continue;
		}

		cout << currentRFC3339Time() << ", ACK, " << received_seq
		     << ", " << base << ", " << window_end << endl;
		acks_remaining--;
		it->second->setAcked(_sender_id, true);
	}
	return 0;
}

int sendPutPacket(int sockfd, sockaddr_in *server_addr, const char *filename) {
	vector<char> send_buffer(1024);
	vector<char> recv_buffer(1024);

	uint16_t cmd = CMD_PUT;
	ssize_t length = strlen(filename);

	memcpy(send_buffer.data(), &cmd, sizeof(uint16_t));
	memcpy(send_buffer.data() + sizeof(uint16_t), filename, length + 1);

	dprintf("Sending PUT packet...\n");

	ssize_t bytes_sent = sendto(sockfd, send_buffer.data(),
	                            length + sizeof(uint16_t), 0,
	                            (struct sockaddr *)server_addr,
	                            sizeof(*server_addr));
	if (bytes_sent < 0) {
		perror("Error sending packet");
		return -1;
	}

	ssize_t bytes_received = recvfrom(sockfd, recv_buffer.data(), 1024, 0,
	                                  NULL, NULL);
	if (bytes_received < 0) {
		dprintf("Bytes received: %ld", bytes_received);
		perror("Cannot detect server");
		return -2;
	}

	if (bytes_received < (ssize_t)(sizeof(uint16_t) + sizeof(uint32_t))) {
		cerr << "Unexpected PUT response size" << endl;
		return -3;
	}

	uint32_t received_seq_number;
	memcpy(&cmd, recv_buffer.data(), sizeof(uint16_t));
	memcpy(&received_seq_number, recv_buffer.data() + sizeof(uint16_t),
	       sizeof(uint32_t));

	if (cmd == CMD_ERROR) {
		printf("Received Error. Error code is: %ud\n", received_seq_number);
		return -4;
	}
	if (cmd != CMD_ACK) {
		cerr << "Invalid ACK packet for PUT" << endl;
		return -5;
	}
	if (received_seq_number != 0) {
		cerr << "Invalid sequence number in PUT ACK" << endl;
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
	}
	if (!S_ISREG(sb.st_mode)) {
		dprintf("Path is not a file: %s\n", file_path);
		return -1;
	}
	return 0;
}

static string formatFileSize(off_t bytes) {
	ostringstream oss;
	oss << fixed << setprecision(1);
	if (bytes < 1024) {
		oss << bytes << " B";
	} else if (bytes < 1024 * 1024) {
		oss << (static_cast<double>(bytes) / 1024.0) << " KB";
	} else {
		oss << (static_cast<double>(bytes) / (1024.0 * 1024.0)) << " MB";
	}
	return oss.str();
}

static double elapsedSeconds(const timeval &start, const timeval &end) {
	return (end.tv_sec - start.tv_sec) +
	       (end.tv_usec - start.tv_usec) / 1'000'000.0;
}

static void printTransferSummary(const string &file_path, int replica_count,
                                 const timeval &start, const timeval &end,
                                 uint64_t retransmits) {
	struct stat sb;
	off_t file_size = 0;
	if (stat(file_path.c_str(), &sb) == 0)
		file_size = sb.st_size;

	double duration = elapsedSeconds(start, end);
	double throughput_mbps = 0.0;
	if (duration > 0.0)
		throughput_mbps =
			(static_cast<double>(file_size) / (1024.0 * 1024.0)) / duration;

	cout << endl;
	cout << "Transfer complete" << endl;
	cout << "  File:        " << file_path << " (" << formatFileSize(file_size)
	     << ")" << endl;
	cout << "  Replicas:    " << replica_count << "/" << replica_count << " OK"
	     << endl;
	cout << "  Duration:    " << fixed << setprecision(2) << duration << "s"
	     << endl;
	cout << "  Retransmits: " << retransmits << endl;
	cout << "  Throughput:  " << fixed << setprecision(2) << throughput_mbps
	     << " MB/s" << endl;
}

typedef struct reader_param_t {
	int mtu;
	int winsz;
	string infile_path;
	GBNWindow *window;
} reader_param;

typedef struct sender_param_t {
	int sender_id;
	pthread_t sender_tid;
	int winsz;
	string server_ip;
	int server_port;
	string outfile_path;
	GBNWindow *window;
} sender_param;

static void *
reader_thread(void *arg) {
	reader_param *param = (reader_param *)arg;
	GBNReader reader(param->mtu, param->winsz, param->infile_path.c_str(),
	                 param->window);
	sleep(1);
	int ret_val = reader.open_file();
	if (ret_val < 0) {
		cerr << "Error: Cannot open input file '" << param->infile_path << "'."
		     << endl;
		return NULL;
	}

	while (1) {
		dprintf("R: filling window\n");
		pthread_mutex_lock(&(param->window->queue_mutex));
		ret_val = reader.fillWindow();
		dprintf("R: fill window complete\n");

		if (ret_val < 0) {
			pthread_mutex_unlock(&(param->window->queue_mutex));
			cerr << "R: read chunk data failure" << endl;
			return NULL;
		}

		if (!param->window->isEOF() && param->window->inFlightCount() == 0) {
			reader.addEOFPacket();
			param->window->setEOF(true);
		}

		dprintf("R: signalling senders, in-flight: %d\n",
		        param->window->inFlightCount());
		pthread_cond_broadcast(&(param->window->sender_cond));
		pthread_mutex_unlock(&(param->window->queue_mutex));

		if (param->window->isEOF() && param->window->inFlightCount() == 0) {
			dprintf("R: transfer complete, breaking loop\n");
			break;
		}

		dprintf("R: waiting on reader cond\n");
		pthread_mutex_lock(&(param->window->queue_mutex));
		struct timespec timeout;
		struct timeval now;

		gettimeofday(&now, NULL);
		timeout.tv_sec = now.tv_sec + 2;
		timeout.tv_nsec = 1000 * now.tv_usec;
		do {
			dprintf("R: active senders: %d\n", param->window->_senders);
			pthread_cond_timedwait(&(param->window->reader_cond),
			                       &(param->window->queue_mutex), &timeout);
		} while (param->window->_senders > 0);
		param->window->deleteFullyAcked();
		pthread_mutex_unlock(&(param->window->queue_mutex));
	}

	reader.close_file();
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

	timeval timeout{};
	timeout.tv_sec = TIMEOUT_SEC;
	timeout.tv_usec = 0;
	if (setsockopt(client_socket, SOL_SOCKET, SO_RCVTIMEO, &timeout,
	               sizeof(timeout)) < 0) {
		perror("Error setting socket timeout");
		close(client_socket);
		return NULL;
	}

	dprintf("S: sending put packet\n");
	int ret_val = sendPutPacket(client_socket, &server_addr,
	                            param->outfile_path.c_str());
	if (ret_val < 0) {
		if (ret_val >= -2) {
			cerr << "Server is down IP " << param->server_ip << " port "
			     << param->server_port << endl;
			exit(5);
		}
		close(client_socket);
		return NULL;
	}

	GBNSender sender(param->sender_id, param->winsz, client_socket,
	                 &server_addr, param->window);
	while (1) {
		dprintf("S: checking window\n");
		pthread_mutex_lock(&(param->window->queue_mutex));
		while (param->window->unackedCount(param->sender_id) == 0 &&
		       !param->window->isEOF())
			pthread_cond_wait(&(param->window->sender_cond),
			                  &(param->window->queue_mutex));

		int unacked = param->window->unackedCount(param->sender_id);
		bool done = (unacked == 0 && param->window->isEOF());
		if (!done)
			param->window->_senders++;

		pthread_mutex_unlock(&(param->window->queue_mutex));
		dprintf("S: processing window\n");

		if (done)
			break;

		ret_val = sender.sendWindow();
		if (ret_val < 0) {
			cerr << "Reached max re-transmission limit IP "
			     << param->server_ip << endl;
			exit(4);
		}

		if (sender.packetsInFlight() == 0) {
			usleep(100000);
			pthread_mutex_lock(&(param->window->queue_mutex));
			param->window->_senders--;
			pthread_cond_signal(&(param->window->reader_cond));
			pthread_mutex_unlock(&(param->window->queue_mutex));
			continue;
		}

		ret_val = sender.recvAcks();
		if (ret_val < 0) {
			if (ret_val == -1) {
				cerr << "Cannot detect server IP " << param->server_ip
				     << " port " << param->server_port << endl;
				exit(3);
			}
			dprintf("receive ACK failure");
			close(client_socket);
			return NULL;
		}

		dprintf("S: signalling reader\n");
		pthread_mutex_lock(&(param->window->queue_mutex));
		param->window->_senders--;
		pthread_cond_signal(&(param->window->reader_cond));
		pthread_mutex_unlock(&(param->window->queue_mutex));
		usleep(10000);
	}
	close(client_socket);
	dprintf("S: sender completed successfully\n");
	return NULL;
}

int readServerConfig(string config_file, vector<pair<string, int>> &arr) {
	ifstream ifs(config_file, ifstream::in);

	while (ifs.good() && !ifs.eof()) {
		char line[256];
		ifs.getline(line, 256);
		if (strlen(line) == 0)
			continue;
		if (line[0] == '#')
			continue;

		char *ip = strtok(line, " ");
		char *port = strtok(NULL, " ");

		if (ip == NULL || port == NULL)
			return -1;
		dprintf("IP: %s, Port: %s\n", ip, port);
		arr.push_back(make_pair(string(ip), stoi(port)));
	}
	ifs.close();
	return 0;
}

int main(int argc, char *argv[]) {
	if (argc != 7) {
		cerr << "Usage: ./myclient <servn> <servaddr.conf> <mss> <winsz> "
		        "<in_file_path> <out_file_path>" << endl;
		return EXIT_FAILURE;
	}

	int servn = stoi(argv[1]);
	string serv_conf = argv[2];
	int mtu = stoi(argv[3]);
	int winsz = stoi(argv[4]);
	string input_file_path = argv[5];
	string output_file_path = argv[6];

	if (mtu <= (int)(sizeof(uint16_t) + sizeof(uint32_t))) {
		cerr << "Required minimum MSS is "
		     << sizeof(uint16_t) + sizeof(uint32_t) + 1 << endl;
		return EXIT_FAILURE;
	}

	if (checkFile(input_file_path.c_str())) {
		cerr << "Invalid Input File" << endl;
		return EXIT_FAILURE;
	}

	vector<pair<string, int>> servers;
	int ret = readServerConfig(serv_conf, servers);
	if (ret == -1) {
		cerr << "Malformed config file!" << endl;
		exit(6);
	}

	int sender_count = servn;
	if (sender_count > (int)servers.size()) {
		cerr << "Calling more servers than provided in your config file!" << endl;
		return EXIT_FAILURE;
	}

	GBNWindow window(sender_count);
	reader_param r_param = {mtu, winsz, input_file_path, &window};

	timeval transfer_start{};
	gettimeofday(&transfer_start, NULL);

	pthread_t reader_tid;
	int s = pthread_create(&reader_tid, NULL, &reader_thread, &r_param);
	if (s != 0)
		handle_error_en(s, "pthread_create for reader");

	sender_param *s_param =
		(sender_param *)calloc(sender_count, sizeof(sender_param));

	for (int i = 0; i < sender_count; i++) {
		pair<string, int> server = servers.at(i);

		s_param[i].sender_id = i;
		s_param[i].winsz = winsz;
		s_param[i].server_ip = server.first;
		s_param[i].server_port = server.second;
		s_param[i].outfile_path = output_file_path;
		s_param[i].window = &window;

		s = pthread_create(&s_param[i].sender_tid, NULL, &sender_thread,
		                   &s_param[i]);
		if (s != 0)
			handle_error_en(s, "pthread_create for sender");
	}

	void *res;
	s = pthread_join(reader_tid, &res);
	if (s != 0)
		handle_error_en(s, "pthread_join for reader");

	for (int i = 0; i < sender_count; i++) {
		s = pthread_join(s_param[i].sender_tid, &res);
		if (s != 0)
			handle_error_en(s, "pthread_join for sender");
	}

	free(s_param);

	timeval transfer_end{};
	gettimeofday(&transfer_end, NULL);
	printTransferSummary(input_file_path, sender_count, transfer_start,
	                     transfer_end, window.retransmits());
	return EXIT_SUCCESS;
}
